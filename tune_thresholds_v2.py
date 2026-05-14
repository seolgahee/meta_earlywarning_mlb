"""
임계치 튜닝 v2 — 지금까지 쌓인 MLB 스냅샷 기반 분포 분석.

목적: "건수" 아니라 "진짜 급등 소재"를 잡는 임계치 도출.
- brand='MLB' 단일 (성인/키즈는 캠페인명 첫글자 M/I로 분리)
- Performance: roas_6h, spend_6h, purch_6h, roas_6h vs roas_prev_6h
- BR: imp_6h, clk_6h, ctr_6h vs ctr_12h

산출: 분포(P50/P75/P90/P95/P99) + 현행 룰 hit + 후보 룰 그리드.
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


def fetch_all() -> pd.DataFrame:
    sql = f"""
    SELECT brand, ad_id, ad_name, campaign_name, adset_name, channel, snapshot_ts,
           impressions_cum, clicks_cum, spend_cum, purchases_cum, revenue_cum
    FROM {TABLE}
    WHERE brand IN ('MLB', 'MLB_KIDS')
    ORDER BY ad_id, snapshot_ts
    """
    df = q(sql)
    df.columns = [c.upper() for c in df.columns]
    return df


def build_rolling_samples(df: pd.DataFrame, window_hours: int) -> pd.DataFrame:
    df = df.sort_values(["AD_ID", "SNAPSHOT_TS"]).copy()
    df["SNAPSHOT_TS"] = pd.to_datetime(df["SNAPSHOT_TS"], utc=True)

    samples = []
    for ad_id, g in df.groupby("AD_ID"):
        rows = g.to_dict("records")
        ts_list = [r["SNAPSHOT_TS"] for r in rows]
        for i, now in enumerate(rows):
            target = now["SNAPSHOT_TS"] - pd.Timedelta(hours=window_hours)
            diffs = [(abs((ts - target).total_seconds()), j) for j, ts in enumerate(ts_list)]
            diffs = [(d, j) for d, j in diffs if d <= 3600 and j != i]
            if not diffs:
                continue
            _, j = min(diffs)
            past = rows[j]
            if past["SNAPSHOT_TS"] >= now["SNAPSHOT_TS"]:
                continue
            samples.append({
                "AD_ID": ad_id, "AD_NAME": now["AD_NAME"], "BRAND": now["BRAND"],
                "CAMPAIGN_NAME": now["CAMPAIGN_NAME"], "NOW_TS": now["SNAPSHOT_TS"],
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
    # brand 컬럼 기준 segment 분리 (성인='MLB', 키즈='MLB_KIDS')
    out["SEG"] = np.where(out["BRAND"] == "MLB_KIDS", "KIDS", "ADULT")
    # BR 검출은 app.py와 동일: 구분자 split 후 토큰 정확 일치 (underscore가 word char라 \b로 안 잡힘)
    import re as _re
    out["IS_BR"] = out["CAMPAIGN_NAME"].fillna("").apply(
        lambda s: "BR" in [t.upper() for t in _re.split(r"[\s_\-|/]+", s)]
    )
    return out


def pct_line(s: pd.Series, label: str, pcts=(50, 75, 90, 95, 99)) -> str:
    s = s.dropna()
    if len(s) == 0:
        return f"  {label}: no data"
    p = {pq: s.quantile(pq / 100) for pq in pcts}
    head = f"  {label}: n={len(s)}, mean={s.mean():.2f}, max={s.max():.2f}"
    body = "    " + ", ".join(f"p{pq}={p[pq]:.2f}" for pq in pcts)
    return head + "\n" + body


def analyze_performance(s6: pd.DataFrame, s12: pd.DataFrame, seg: str):
    perf6  = s6[(s6["SEG"]  == seg) & (~s6["IS_BR"])].copy()
    perf12 = s12[(s12["SEG"] == seg) & (~s12["IS_BR"])].copy()
    if perf6.empty:
        print(f"\n[Performance {seg}] 데이터 없음")
        return

    key = ["AD_ID", "NOW_TS"]
    merged = perf6.merge(
        perf12[key + ["SPEND", "PURCH", "REV"]].rename(
            columns={"SPEND": "SPEND_12H", "PURCH": "PURCH_12H", "REV": "REV_12H"}
        ),
        on=key, how="inner",
    )
    merged["SPEND_PREV"] = (merged["SPEND_12H"] - merged["SPEND"]).clip(lower=0)
    merged["REV_PREV"]   = (merged["REV_12H"]   - merged["REV"]).clip(lower=0)
    merged["ROAS_PREV"]  = np.where(merged["SPEND_PREV"] > 0, merged["REV_PREV"] / merged["SPEND_PREV"], np.nan)
    merged["ROAS_RATIO"] = np.where(merged["ROAS_PREV"] > 0, merged["ROAS"] / merged["ROAS_PREV"], np.nan)

    print(f"\n{'─' * 70}")
    print(f"  Performance — {seg} (perf6 n={len(perf6)}, perf6+prev n={len(merged)})")
    print(f"{'─' * 70}")
    print("[6h 절대값 분포]")
    print(pct_line(perf6["SPEND"], "SPEND_6H (원)"))
    print(pct_line(perf6["PURCH"], "PURCH_6H (건)"))
    print(pct_line(perf6["ROAS"],  "ROAS_6H (배)"))
    print(pct_line(perf6["CTR"],   "CTR_6H"))

    print("\n[추세 비교 분포 (직전 6h 대비)]")
    print(pct_line(merged["SPEND_PREV"], "SPEND_PREV_6H (원)"))
    print(pct_line(merged["ROAS_RATIO"], "ROAS_6H / ROAS_PREV_6H (배)"))

    # 현행 임계치 ([ad_threshold_tuning_v1] 기준)
    cur = {"ADULT": dict(roas=4.0, spend=10000, purch=3),
           "KIDS":  dict(roas=5.0, spend=5000,  purch=2)}.get(seg)
    if cur:
        m = (perf6["ROAS"] >= cur["roas"]) & (perf6["SPEND"] >= cur["spend"]) & (perf6["PURCH"] >= cur["purch"])
        hit = perf6[m]
        print(f"\n[현행 임계치] roas>={cur['roas']}, spend>={cur['spend']:,}, purch>={cur['purch']}")
        print(f"  hit={len(hit)} ({len(hit)/max(1,len(perf6))*100:.2f}%), "
              f"ads={hit['AD_ID'].nunique()}, hours={hit['NOW_TS'].dt.floor('h').nunique() if len(hit) else 0}")

    print("\n[후보 임계치 그리드]")
    print(f"  {'roas':>6} {'spend':>8} {'purch':>6} {'hits':>6} {'ads':>5} {'hours':>6}")
    for roas in [3.0, 4.0, 5.0, 6.0, 8.0]:
        for spend in [5000, 10000, 20000, 30000]:
            for purch in [2, 3, 4, 5]:
                m = (perf6["ROAS"] >= roas) & (perf6["SPEND"] >= spend) & (perf6["PURCH"] >= purch)
                hit = perf6[m]
                if len(hit) == 0:
                    continue
                print(f"  {roas:>6.1f} {spend:>8,} {purch:>6} {len(hit):>6} {hit['AD_ID'].nunique():>5} "
                      f"{hit['NOW_TS'].dt.floor('h').nunique():>6}")


def analyze_br(s6: pd.DataFrame, s12: pd.DataFrame, seg: str):
    br6  = s6[(s6["SEG"]  == seg) & (s6["IS_BR"])].copy()
    br12 = s12[(s12["SEG"] == seg) & (s12["IS_BR"])].copy()
    if br6.empty:
        print(f"\n[BR {seg}] 데이터 없음")
        return

    key = ["AD_ID", "NOW_TS"]
    merged = br6.merge(
        br12[key + ["CTR", "IMP", "CLK"]].rename(
            columns={"CTR": "CTR_12H", "IMP": "IMP_12H", "CLK": "CLK_12H"}
        ),
        on=key, how="inner",
    )
    merged["CTR_RATIO"] = np.where(merged["CTR_12H"] > 0, merged["CTR"] / merged["CTR_12H"], np.nan)

    print(f"\n{'─' * 70}")
    print(f"  BR 브랜딩 — {seg} (br6 n={len(br6)}, br6+12h n={len(merged)})")
    print(f"{'─' * 70}")
    print("[6h 분포]")
    print(pct_line(br6["IMP"], "IMP_6H"))
    print(pct_line(br6["CLK"], "CLK_6H"))
    print(pct_line(br6["CTR"], "CTR_6H"))
    print("\n[CTR 추세 분포]")
    print(pct_line(merged["CTR_RATIO"], "CTR_6H / CTR_12H (배)"))

    # 현행 임계치 (BR_ALERT_CONDITIONS_BY_BRAND 실제 코드값)
    cur = {"ADULT": dict(imp=2000, clk=30,  surge=1.05, drop=0.85),
           "KIDS":  dict(imp=1000, clk=20,  surge=1.05, drop=0.85)}.get(seg)
    print(f"\n[현행 임계치] imp>={cur['imp']:,} AND clk>={cur['clk']} AND ctr_ratio>={cur['surge']} (SURGE) / <={cur['drop']} (DROP)")
    m_entry = (br6["IMP"] >= cur["imp"]) & (br6["CLK"] >= cur["clk"])
    print(f"  진입 통과(br6 기준): {m_entry.sum()} / {len(br6)} ({m_entry.sum()/max(1,len(br6))*100:.2f}%)")
    m_e2 = (merged["IMP"] >= cur["imp"]) & (merged["CLK"] >= cur["clk"])
    surge = merged[m_e2 & (merged["CTR_RATIO"] >= cur["surge"])]
    drop  = merged[m_e2 & (merged["CTR_RATIO"] <= cur["drop"])]
    print(f"  SURGE hit: {len(surge)}, ads={surge['AD_ID'].nunique()}, "
          f"hours={surge['NOW_TS'].dt.floor('h').nunique() if len(surge) else 0}")
    print(f"  DROP  hit: {len(drop)}, ads={drop['AD_ID'].nunique()}, "
          f"hours={drop['NOW_TS'].dt.floor('h').nunique() if len(drop) else 0}")

    print("\n[후보 임계치 그리드 — SURGE]")
    print(f"  {'imp_min':>8} {'clk_min':>8} {'ratio':>6} {'hits':>6} {'ads':>5} {'hours':>6}")
    for imp in [500, 1000, 1500, 2000, 3000]:
        for clk in [10, 20, 30, 50]:
            for ratio in [1.05, 1.10, 1.20, 1.30, 1.50]:
                m = (merged["IMP"] >= imp) & (merged["CLK"] >= clk) & (merged["CTR_RATIO"] >= ratio)
                hit = merged[m]
                if len(hit) == 0:
                    continue
                print(f"  {imp:>8,} {clk:>8,} {ratio:>6.2f} {len(hit):>6} {hit['AD_ID'].nunique():>5} "
                      f"{hit['NOW_TS'].dt.floor('h').nunique():>6}")

    print("\n[후보 임계치 그리드 — DROP]")
    print(f"  {'imp_min':>8} {'clk_min':>8} {'ratio':>6} {'hits':>6} {'ads':>5} {'hours':>6}")
    for imp in [500, 1000, 1500, 2000, 3000]:
        for clk in [10, 20, 30, 50]:
            for ratio in [0.95, 0.90, 0.85, 0.80, 0.70]:
                m = (merged["IMP"] >= imp) & (merged["CLK"] >= clk) & (merged["CTR_RATIO"] <= ratio)
                hit = merged[m]
                if len(hit) == 0:
                    continue
                print(f"  {imp:>8,} {clk:>8,} {ratio:>6.2f} {len(hit):>6} {hit['AD_ID'].nunique():>5} "
                      f"{hit['NOW_TS'].dt.floor('h').nunique():>6}")


def main():
    kst = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M KST")
    print(f"[분석 시각] {kst}")
    print(f"[Snowflake] {TABLE}")

    raw = fetch_all()
    if raw.empty:
        print("데이터 없음"); return
    snap_min = pd.to_datetime(raw['SNAPSHOT_TS']).min()
    snap_max = pd.to_datetime(raw['SNAPSHOT_TS']).max()
    days = (snap_max - snap_min).total_seconds() / 86400
    print(f"  raw rows={len(raw)}, ads={raw['AD_ID'].nunique()}, "
          f"snaps={raw['SNAPSHOT_TS'].nunique()}, span={days:.1f}일")
    print(f"  range: {snap_min} ~ {snap_max}")

    s6  = build_rolling_samples(raw, 6)
    s12 = build_rolling_samples(raw, 12)
    print(f"  rolling 6h samples={len(s6)}, 12h samples={len(s12)}")

    for seg in ["ADULT", "KIDS"]:
        analyze_performance(s6, s12, seg)
        analyze_br(s6, s12, seg)


if __name__ == "__main__":
    main()
