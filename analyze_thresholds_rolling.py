"""
임계치 튜닝 — Rolling 6h / 12h 윈도우 분석.
모든 hourly 스냅샷을 기준점으로 잡아 6h/12h 이전 스냅샷과의 델타를 계산.
ad 당 최대 (총 스냅샷 - 6)개 샘플 확보 → 더 robust한 분포 추정.
"""
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import numpy as np
from dotenv import load_dotenv
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key, Encoding, PrivateFormat, NoEncryption,
)

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")

SF = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE", "PU_PF"),
}
TABLE = f"{SF['database']}.{SF['schema']}.{os.getenv('SNOWFLAKE_TABLE', 'META_AD_SNAPSHOT')}"


def _pk():
    with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), "rb") as f:
        key = load_pem_private_key(f.read(), password=None, backend=default_backend())
    return key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())


def q(sql):
    conn = snowflake.connector.connect(private_key=_pk(), **SF)
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


def fetch_all(brand: str) -> pd.DataFrame:
    """해당 브랜드의 전체 스냅샷 raw 데이터."""
    sql = f"""
    SELECT ad_id, ad_name, campaign_name, adset_name, channel, snapshot_ts,
           impressions_cum, clicks_cum, spend_cum, purchases_cum, revenue_cum
    FROM {TABLE}
    WHERE brand = '{brand}'
    ORDER BY ad_id, snapshot_ts
    """
    df = q(sql)
    df.columns = [c.upper() for c in df.columns]
    return df


def build_rolling_samples(df: pd.DataFrame, window_hours: int) -> pd.DataFrame:
    """ad별로 모든 (now_ts, past_ts) 쌍을 만들어 델타 샘플 생성. now와 past 사이가 ±1h 이내로 window_hours 차이."""
    df = df.sort_values(["AD_ID", "SNAPSHOT_TS"]).copy()
    df["SNAPSHOT_TS"] = pd.to_datetime(df["SNAPSHOT_TS"], utc=True)

    samples = []
    for ad_id, g in df.groupby("AD_ID"):
        rows = g.to_dict("records")
        ts_list = [r["SNAPSHOT_TS"] for r in rows]
        for i, now in enumerate(rows):
            target = now["SNAPSHOT_TS"] - pd.Timedelta(hours=window_hours)
            # 가장 가까운 과거 스냅샷 찾기 (±1h 허용)
            diffs = [(abs((ts - target).total_seconds()), j) for j, ts in enumerate(ts_list)]
            diffs = [(d, j) for d, j in diffs if d <= 3600 and j != i]
            if not diffs:
                continue
            _, j = min(diffs)
            past = rows[j]
            if past["SNAPSHOT_TS"] >= now["SNAPSHOT_TS"]:
                continue
            samples.append({
                "AD_ID": ad_id,
                "AD_NAME": now["AD_NAME"],
                "CAMPAIGN_NAME": now["CAMPAIGN_NAME"],
                "ADSET_NAME": now["ADSET_NAME"],
                "NOW_TS": now["SNAPSHOT_TS"],
                "PAST_TS": past["SNAPSHOT_TS"],
                "SPEND": max(0, (now["SPEND_CUM"] or 0) - (past["SPEND_CUM"] or 0)),
                "PURCH": max(0, (now["PURCHASES_CUM"] or 0) - (past["PURCHASES_CUM"] or 0)),
                "REV":   max(0, (now["REVENUE_CUM"] or 0) - (past["REVENUE_CUM"] or 0)),
                "IMP":   max(0, (now["IMPRESSIONS_CUM"] or 0) - (past["IMPRESSIONS_CUM"] or 0)),
                "CLK":   max(0, (now["CLICKS_CUM"] or 0) - (past["CLICKS_CUM"] or 0)),
            })
    out = pd.DataFrame(samples)
    if out.empty:
        return out
    out["ROAS"] = np.where(out["SPEND"] > 0, out["REV"] / out["SPEND"], np.nan)
    out["CTR"]  = np.where(out["IMP"]  > 0, out["CLK"] / out["IMP"],  np.nan)
    out["IS_BR"] = out["CAMPAIGN_NAME"].fillna("").str.contains(r"\bBR\b", regex=True, case=False)
    return out


def pct_line(s: pd.Series, label: str, pcts=(50, 75, 80, 85, 90, 95, 99)) -> str:
    s = s.dropna()
    if len(s) == 0:
        return f"  {label}: no data"
    p = {q: s.quantile(q / 100) for q in pcts}
    head = f"  {label}: n={len(s)}, mean={s.mean():.2f}, max={s.max():.2f}"
    body = "    " + ", ".join(f"p{q}={p[q]:.2f}" for q in pcts)
    return head + "\n" + body


def simulate_rule(samples: pd.DataFrame, *, roas_min, spend_min, purch_min) -> dict:
    """주어진 임계치로 sample 수, 광고 수, 캠페인 수 등을 산출."""
    m = (samples["ROAS"] >= roas_min) & (samples["SPEND"] >= spend_min) & (samples["PURCH"] >= purch_min)
    hit = samples[m]
    return {
        "rule": f"roas>={roas_min}, spend>={spend_min:,}, purch>={purch_min}",
        "n_hit_samples": len(hit),
        "pct_hits": (len(hit) / max(1, len(samples))) * 100,
        "n_ads": hit["AD_ID"].nunique(),
        "n_campaigns": hit["CAMPAIGN_NAME"].nunique(),
        "n_hours": hit["NOW_TS"].dt.floor("h").nunique() if len(hit) else 0,
    }


def analyze_brand(brand: str, label: str):
    print(f"\n{'=' * 80}")
    print(f"  {label} (brand={brand}) — Rolling window 분석")
    print(f"{'=' * 80}")

    raw = fetch_all(brand)
    if raw.empty:
        print("  데이터 없음")
        return None
    print(f"  raw rows={len(raw)}, distinct ads={raw['AD_ID'].nunique()}, "
          f"distinct snaps={raw['SNAPSHOT_TS'].nunique()}, "
          f"snap range={raw['SNAPSHOT_TS'].min()} ~ {raw['SNAPSHOT_TS'].max()}")

    s6 = build_rolling_samples(raw, 6)
    s12 = build_rolling_samples(raw, 12)
    perf6 = s6[~s6["IS_BR"]] if "IS_BR" in s6 else s6
    perf12 = s12[~s12["IS_BR"]] if "IS_BR" in s12 else s12

    print(f"\n  rolling 6h 샘플 수: {len(perf6)} (광고 {perf6['AD_ID'].nunique()}개)")
    print(f"  rolling 12h 샘플 수: {len(perf12)} (광고 {perf12['AD_ID'].nunique()}개)")

    print("\n[Performance 6h 델타 분포]")
    print(pct_line(perf6["SPEND"], "SPEND_6H (원)"))
    print(pct_line(perf6["PURCH"], "PURCH_6H (건)"))
    print(pct_line(perf6["IMP"],   "IMP_6H"))
    print(pct_line(perf6["CLK"],   "CLK_6H"))
    print(pct_line(perf6["ROAS"],  "ROAS_6H (배)"))
    print(pct_line(perf6["CTR"],   "CTR_6H"))

    # purch>=N 별 분포
    print("\n[6h purch별 cohort 분포]")
    for n in [1, 2, 3, 4, 5]:
        sub = perf6[perf6["PURCH"] >= n]
        if len(sub) == 0:
            print(f"  purch>={n}: 0개")
            continue
        print(f"  purch>={n}: n={len(sub)}, "
              f"spend(p50={sub['SPEND'].quantile(.5):.0f}, p90={sub['SPEND'].quantile(.9):.0f}), "
              f"roas(p50={sub['ROAS'].quantile(.5):.2f}, p90={sub['ROAS'].quantile(.9):.2f}), "
              f"광고={sub['AD_ID'].nunique()}")

    print("\n[현행 룰 시뮬레이션 — roas>=3.0, spend>=10,000, purch>=2]")
    cur = simulate_rule(perf6, roas_min=3.0, spend_min=10000, purch_min=2)
    print(f"  {cur}")

    print("\n[튜닝 후보 시뮬레이션 — hit rate 표]")
    print(f"  {'rule':<48} {'hits':>6} {'pct':>7} {'ads':>5} {'camp':>5} {'hours':>6}")
    rule_grid = []
    for purch_min in [1, 2, 3]:
        for spend_min in [5000, 10000, 15000, 20000]:
            for roas_min in [3.0, 4.0, 5.0]:
                rule_grid.append((roas_min, spend_min, purch_min))
    for c in rule_grid:
        r = simulate_rule(perf6, roas_min=c[0], spend_min=c[1], purch_min=c[2])
        print(f"  {r['rule']:<48} {r['n_hit_samples']:>6} {r['pct_hits']:>6.1f}% "
              f"{r['n_ads']:>5} {r['n_campaigns']:>5} {r['n_hours']:>6}")

    return {"raw": raw, "s6": s6, "s12": s12}


def main():
    kst = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M KST")
    print(f"[분석 시각] {kst}")
    print(f"[Snowflake] {TABLE}")

    analyze_brand("MLB", "MLB 성인")
    analyze_brand("MLB_KIDS", "MLB 키즈")


if __name__ == "__main__":
    main()
