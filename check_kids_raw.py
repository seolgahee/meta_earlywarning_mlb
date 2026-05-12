"""
키즈(I) 광고 6h 델타 — 캠페인명 첫글자 'I' 기준으로 재확인.
BRAND 컬럼이 아니라 CAMPAIGN_NAME split[0] 기준.
"""
import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key, Encoding, PrivateFormat, NoEncryption,
)

load_dotenv()
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 240)
pd.set_option("display.max_rows", 200)

SF_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SF_USER = os.getenv("SNOWFLAKE_USER")
SF_PK_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
SF_DB = os.getenv("SNOWFLAKE_DATABASE")
SF_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SF_WH = os.getenv("SNOWFLAKE_WAREHOUSE")
SF_ROLE = os.getenv("SNOWFLAKE_ROLE", "PU_PF")
SF_TABLE = os.getenv("SNOWFLAKE_TABLE", "META_AD_SNAPSHOT")


def _pk():
    with open(SF_PK_PATH, "rb") as f:
        key = load_pem_private_key(f.read(), password=None, backend=default_backend())
    return key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())


def conn():
    return snowflake.connector.connect(
        account=SF_ACCOUNT, user=SF_USER, private_key=_pk(),
        warehouse=SF_WH, database=SF_DB, schema=SF_SCHEMA, role=SF_ROLE,
    )


def main():
    sql = f"""
    WITH ranked AS (
      SELECT ad_id, ad_name, campaign_name, adset_name, channel, brand, snapshot_ts,
             impressions_cum, clicks_cum, spend_cum, purchases_cum, revenue_cum,
             ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY snapshot_ts DESC) AS rn
      FROM {SF_DB}.{SF_SCHEMA}.{SF_TABLE}
      WHERE snapshot_ts >= DATEADD('hour', -36, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))
    ),
    now_snap AS (SELECT * FROM ranked WHERE rn = 1),
    past6 AS (
      SELECT ad_id, impressions_cum, clicks_cum, spend_cum, purchases_cum, revenue_cum,
        ROW_NUMBER() OVER (
          PARTITION BY ad_id
          ORDER BY ABS(DATEDIFF('minute', snapshot_ts,
                       DATEADD('hour', -6, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))))
        ) AS rn
      FROM {SF_DB}.{SF_SCHEMA}.{SF_TABLE}
      WHERE snapshot_ts >= DATEADD('hour', -8, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))
        AND snapshot_ts <= DATEADD('hour', -4, CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))
    )
    SELECT n.ad_id, n.ad_name, n.campaign_name, n.adset_name, n.channel, n.brand,
           n.snapshot_ts AS snap_now,
           n.impressions_cum AS imp_now, n.clicks_cum AS clk_now,
           n.spend_cum AS spend_now, n.purchases_cum AS purch_now, n.revenue_cum AS rev_now,
           p6.impressions_cum AS imp_6h_past, p6.clicks_cum AS clk_6h_past,
           p6.spend_cum AS spend_6h_past, p6.purchases_cum AS purch_6h_past,
           p6.revenue_cum AS rev_6h_past
    FROM now_snap n
    LEFT JOIN (SELECT * FROM past6 WHERE rn=1) p6 ON p6.ad_id = n.ad_id
    """
    c = conn()
    try:
        df = pd.read_sql(sql, c)
    finally:
        c.close()
    df.columns = [x.upper() for x in df.columns]

    # 캠페인명 split[0] 기준으로 token 부여
    df["TOKEN"] = df["CAMPAIGN_NAME"].fillna("").apply(
        lambda x: x.split("_")[0].strip().upper() if x else ""
    )

    print(f"전체 광고 수: {len(df)}")
    print(f"\nTOKEN별 분포:")
    print(df["TOKEN"].value_counts())
    print(f"\nBRAND별 분포:")
    print(df["BRAND"].value_counts())
    print(f"\nTOKEN x BRAND 교차표:")
    print(pd.crosstab(df["TOKEN"], df["BRAND"]))

    # ===== 키즈(I) 광고만 =====
    kids = df[df["TOKEN"] == "I"].copy()
    for col in ["IMP_6H_PAST", "CLK_6H_PAST", "SPEND_6H_PAST", "PURCH_6H_PAST", "REV_6H_PAST"]:
        kids[col] = pd.to_numeric(kids[col], errors="coerce").fillna(0)
    for col in ["IMP_NOW", "CLK_NOW", "SPEND_NOW", "PURCH_NOW", "REV_NOW"]:
        kids[col] = pd.to_numeric(kids[col], errors="coerce").fillna(0)

    kids["IMP_6H"] = (kids["IMP_NOW"] - kids["IMP_6H_PAST"]).clip(lower=0)
    kids["CLK_6H"] = (kids["CLK_NOW"] - kids["CLK_6H_PAST"]).clip(lower=0)
    kids["SPEND_6H"] = (kids["SPEND_NOW"] - kids["SPEND_6H_PAST"]).clip(lower=0)
    kids["PURCH_6H"] = (kids["PURCH_NOW"] - kids["PURCH_6H_PAST"]).clip(lower=0)
    kids["REV_6H"] = (kids["REV_NOW"] - kids["REV_6H_PAST"]).clip(lower=0)
    kids["ROAS_6H"] = kids.apply(
        lambda r: round(r["REV_6H"] / r["SPEND_6H"], 2) if r["SPEND_6H"] > 0 else None, axis=1
    )

    print(f"\n{'='*70}")
    print(f"키즈(TOKEN=I) 광고: {len(kids)}개")
    print(f"{'='*70}")

    # 누적 vs 6h delta 비교용 — purchases_cum과 PURCH_6H 같이 보기
    show = kids[["CAMPAIGN_NAME", "ADSET_NAME", "AD_NAME", "CHANNEL", "BRAND",
                 "IMP_6H", "CLK_6H", "SPEND_6H", "PURCH_6H", "REV_6H", "ROAS_6H",
                 "PURCH_NOW", "PURCH_6H_PAST", "REV_NOW", "REV_6H_PAST"]].copy()
    show = show.sort_values("PURCH_6H", ascending=False)

    print("\n[키즈 6h 델타 TOP 20 — 구매순]")
    print(show.head(20).to_string(index=False))

    print("\n[키즈 누적 구매(NOW)가 0보다 큰 광고 — 전체]")
    has_purch = kids[kids["PURCH_NOW"] > 0][["CAMPAIGN_NAME", "AD_NAME", "PURCH_NOW", "PURCH_6H_PAST", "PURCH_6H",
                                              "REV_NOW", "SPEND_NOW", "BRAND"]]
    print(f"총 {len(has_purch)}개")
    print(has_purch.to_string(index=False))

    # 합계
    print(f"\n[키즈 합계]")
    print(f"  6h spend 합계: {kids['SPEND_6H'].sum():,.0f}원")
    print(f"  6h purchases 합계: {kids['PURCH_6H'].sum():.0f}건")
    print(f"  6h revenue 합계: {kids['REV_6H'].sum():,.0f}원")
    print(f"  6h ROAS(합계 기준): {(kids['REV_6H'].sum()/kids['SPEND_6H'].sum() if kids['SPEND_6H'].sum()>0 else 0):.2f}")
    print(f"  누적 purchases 합계: {kids['PURCH_NOW'].sum():.0f}건")
    print(f"  누적 revenue 합계: {kids['REV_NOW'].sum():,.0f}원")


if __name__ == "__main__":
    main()
