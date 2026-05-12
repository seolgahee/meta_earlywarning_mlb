"""
임계치 튜닝 — 최근 6h / 12h 델타 분포 분석
브랜드별(MLB 성인 / MLB_KIDS) 광고 단위 분포 percentile 산출.
"""
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key, Encoding, PrivateFormat, NoEncryption,
)

load_dotenv()

SF_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SF_USER = os.getenv("SNOWFLAKE_USER")
SF_PK_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
SF_DB = os.getenv("SNOWFLAKE_DATABASE")
SF_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SF_WH = os.getenv("SNOWFLAKE_WAREHOUSE")
SF_ROLE = os.getenv("SNOWFLAKE_ROLE", "PU_PF")
SF_TABLE = os.getenv("SNOWFLAKE_TABLE", "META_AD_SNAPSHOT")


def _pk_bytes():
    with open(SF_PK_PATH, "rb") as f:
        key = load_pem_private_key(f.read(), password=None, backend=default_backend())
    return key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())


def get_conn():
    return snowflake.connector.connect(
        account=SF_ACCOUNT, user=SF_USER, private_key=_pk_bytes(),
        warehouse=SF_WH, database=SF_DB, schema=SF_SCHEMA, role=SF_ROLE,
    )


def fetch_delta_distribution(brand: str) -> pd.DataFrame:
    """가장 최신 snapshot_ts(now)과 6h, 12h 전 스냅샷의 ad_id별 누적값 diff."""
    sql = f"""
    WITH ranked AS (
      SELECT
        ad_id, ad_name, campaign_name, adset_name, channel, snapshot_ts,
        impressions_cum, clicks_cum, spend_cum, purchases_cum, revenue_cum,
        ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY snapshot_ts DESC) AS rn_now
      FROM {SF_DB}.{SF_SCHEMA}.{SF_TABLE}
      WHERE brand = '{brand}'
        AND snapshot_ts >= DATEADD('hour', -36, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))
    ),
    now_snap AS (
      SELECT * FROM ranked WHERE rn_now = 1
    ),
    past6 AS (
      SELECT ad_id, impressions_cum, clicks_cum, spend_cum, purchases_cum, revenue_cum,
        ROW_NUMBER() OVER (
          PARTITION BY ad_id
          ORDER BY ABS(DATEDIFF('minute', snapshot_ts,
                       DATEADD('hour', -6, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))))
        ) AS rn
      FROM {SF_DB}.{SF_SCHEMA}.{SF_TABLE}
      WHERE brand = '{brand}'
        AND snapshot_ts >= DATEADD('hour', -8, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))
        AND snapshot_ts <= DATEADD('hour', -4, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))
    ),
    past12 AS (
      SELECT ad_id, impressions_cum, clicks_cum, spend_cum, purchases_cum, revenue_cum,
        ROW_NUMBER() OVER (
          PARTITION BY ad_id
          ORDER BY ABS(DATEDIFF('minute', snapshot_ts,
                       DATEADD('hour', -12, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))))
        ) AS rn
      FROM {SF_DB}.{SF_SCHEMA}.{SF_TABLE}
      WHERE brand = '{brand}'
        AND snapshot_ts >= DATEADD('hour', -14, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))
        AND snapshot_ts <= DATEADD('hour', -10, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))
    )
    SELECT
      n.ad_id, n.ad_name, n.campaign_name, n.adset_name, n.channel, n.snapshot_ts,
      n.impressions_cum, n.clicks_cum, n.spend_cum, n.purchases_cum, n.revenue_cum,
      p6.impressions_cum AS imp_6h_past, p6.clicks_cum AS clk_6h_past,
      p6.spend_cum AS spend_6h_past, p6.purchases_cum AS purch_6h_past,
      p6.revenue_cum AS rev_6h_past,
      p12.impressions_cum AS imp_12h_past, p12.clicks_cum AS clk_12h_past,
      p12.spend_cum AS spend_12h_past, p12.purchases_cum AS purch_12h_past,
      p12.revenue_cum AS rev_12h_past
    FROM now_snap n
    LEFT JOIN (SELECT * FROM past6 WHERE rn = 1) p6 ON p6.ad_id = n.ad_id
    LEFT JOIN (SELECT * FROM past12 WHERE rn = 1) p12 ON p12.ad_id = n.ad_id
    """
    conn = get_conn()
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    df.columns = [c.upper() for c in df.columns]
    return df


def compute_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["IMP_6H_PAST", "CLK_6H_PAST", "SPEND_6H_PAST", "PURCH_6H_PAST", "REV_6H_PAST",
                "IMP_12H_PAST", "CLK_12H_PAST", "SPEND_12H_PAST", "PURCH_12H_PAST", "REV_12H_PAST"]:
        df[col] = df[col].fillna(0)

    df["IMP_6H"] = (df["IMPRESSIONS_CUM"] - df["IMP_6H_PAST"]).clip(lower=0)
    df["CLK_6H"] = (df["CLICKS_CUM"] - df["CLK_6H_PAST"]).clip(lower=0)
    df["SPEND_6H"] = (df["SPEND_CUM"] - df["SPEND_6H_PAST"]).clip(lower=0)
    df["PURCH_6H"] = (df["PURCHASES_CUM"] - df["PURCH_6H_PAST"]).clip(lower=0)
    df["REV_6H"] = (df["REVENUE_CUM"] - df["REV_6H_PAST"]).clip(lower=0)

    df["IMP_12H"] = (df["IMPRESSIONS_CUM"] - df["IMP_12H_PAST"]).clip(lower=0)
    df["CLK_12H"] = (df["CLICKS_CUM"] - df["CLK_12H_PAST"]).clip(lower=0)
    df["SPEND_12H"] = (df["SPEND_CUM"] - df["SPEND_12H_PAST"]).clip(lower=0)
    df["PURCH_12H"] = (df["PURCHASES_CUM"] - df["PURCH_12H_PAST"]).clip(lower=0)
    df["REV_12H"] = (df["REVENUE_CUM"] - df["REV_12H_PAST"]).clip(lower=0)

    df["ROAS_6H"] = df.apply(lambda r: (r["REV_6H"] / r["SPEND_6H"]) if r["SPEND_6H"] > 0 else None, axis=1)
    df["ROAS_12H"] = df.apply(lambda r: (r["REV_12H"] / r["SPEND_12H"]) if r["SPEND_12H"] > 0 else None, axis=1)
    df["CTR_6H"] = df.apply(lambda r: (r["CLK_6H"] / r["IMP_6H"]) if r["IMP_6H"] > 0 else None, axis=1)
    df["CTR_12H"] = df.apply(lambda r: (r["CLK_12H"] / r["IMP_12H"]) if r["IMP_12H"] > 0 else None, axis=1)

    df["IS_BR"] = df["CAMPAIGN_NAME"].fillna("").str.contains(r"\bBR\b", regex=True, case=False)
    return df


def percentile_report(series: pd.Series, label: str, pcts=(50, 75, 80, 85, 90, 95, 99)) -> str:
    s = series.dropna()
    if len(s) == 0:
        return f"  {label}: no data"
    lines = [f"  {label}: n={len(s)}, min={s.min():.2f}, max={s.max():.2f}, mean={s.mean():.2f}"]
    p = {q: s.quantile(q / 100) for q in pcts}
    lines.append("    " + ", ".join(f"p{q}={p[q]:.2f}" for q in pcts))
    return "\n".join(lines)


def analyze_brand(df_raw: pd.DataFrame, brand_label: str):
    df = compute_metrics(df_raw)
    perf = df[~df["IS_BR"]]
    br = df[df["IS_BR"]]

    print(f"\n{'=' * 70}")
    print(f"  {brand_label}: 총 광고 {len(df)}개 (Performance {len(perf)} / BR {len(br)})")
    print(f"{'=' * 70}")

    print("\n[Performance 광고 분포 — 6h 델타]")
    print(percentile_report(perf["SPEND_6H"], "SPEND_6H (원)"))
    print(percentile_report(perf["PURCH_6H"], "PURCHASES_6H (건)"))
    print(percentile_report(perf["IMP_6H"], "IMPRESSIONS_6H"))
    print(percentile_report(perf["CLK_6H"], "CLICKS_6H"))
    print(percentile_report(perf["ROAS_6H"], "ROAS_6H (배)"))
    print(percentile_report(perf["CTR_6H"], "CTR_6H"))

    print("\n[Performance 광고 분포 — 12h 델타]")
    print(percentile_report(perf["SPEND_12H"], "SPEND_12H (원)"))
    print(percentile_report(perf["PURCH_12H"], "PURCHASES_12H (건)"))
    print(percentile_report(perf["ROAS_12H"], "ROAS_12H (배)"))

    print("\n[현재 OPP_FILTER 통과 광고 개수 (roas_6h>=3.0, spend_6h>=10000, purch_6h>=2)]")
    opp_pass = perf[
        (perf["ROAS_6H"] >= 3.0) & (perf["SPEND_6H"] >= 10000) & (perf["PURCH_6H"] >= 2)
    ]
    print(f"  통과: {len(opp_pass)}개 / Performance 전체 {len(perf)}개")
    if len(opp_pass) > 0:
        print("  대표 ad 5개:")
        for _, r in opp_pass.head(5).iterrows():
            print(f"    - {r['CAMPAIGN_NAME']} / spend={r['SPEND_6H']:.0f}, purch={r['PURCH_6H']:.0f}, roas={r['ROAS_6H']:.2f}")

    if len(br) > 0:
        print("\n[BR 광고 분포 — 6h 델타]")
        print(percentile_report(br["IMP_6H"], "IMPRESSIONS_6H"))
        print(percentile_report(br["CLK_6H"], "CLICKS_6H"))
        print(percentile_report(br["CTR_6H"], "CTR_6H"))

    print("\n[KILL 후보 — spend_12h>=150000 & roas_12h<=1.2]")
    kill = perf[(perf["SPEND_12H"] >= 150000) & (perf["ROAS_12H"].fillna(0) <= 1.2)]
    print(f"  현재 룰 매치: {len(kill)}개")

    return df


def main():
    kst = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M KST")
    print(f"[분석 시각] {kst}")
    print(f"[Snowflake] {SF_DB}.{SF_SCHEMA}.{SF_TABLE}")

    for brand in ["MLB", "MLB_KIDS"]:
        try:
            df = fetch_delta_distribution(brand)
        except Exception as e:
            print(f"\n[오류] brand={brand} 조회 실패: {e}")
            continue
        if df.empty:
            print(f"\n[정보] brand={brand} — 데이터 없음 (최근 36h 내 스냅샷 없음)")
            continue
        analyze_brand(df, brand)


if __name__ == "__main__":
    main()
