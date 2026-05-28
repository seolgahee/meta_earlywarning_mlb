"""
2026-05-28 — 최근 7일 스냅샷 기준 임계치 재정의 (전체 4개 카테고리).
- Performance (성인/키즈): OPP_FILTERS 진입게이트 + ACTION_CONDITIONS 분기
- BR (성인/키즈): CTR surge/drop ratio
- Kill: roas_12h / spend_12h

균형 타깃: 성인 1~2건/일, 키즈 0.5~1건/일 (= 7일 7~14건 / 3.5~7건)
"""
import os
import sys
from datetime import datetime, timedelta, timezone
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
DAYS = 7


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


def fetch_recent(brand: str, days: int) -> pd.DataFrame:
    sql = f"""
    SELECT ad_id, ad_name, campaign_name, adset_name, channel, snapshot_ts,
           impressions_cum, clicks_cum, spend_cum, purchases_cum, revenue_cum
    FROM {TABLE}
    WHERE brand = '{brand}'
      AND snapshot_ts >= DATEADD(day, -{days}, CURRENT_TIMESTAMP())
    ORDER BY ad_id, snapshot_ts
    """
    df = q(sql)
    df.columns = [c.upper() for c in df.columns]
    return df


def build_rolling(df: pd.DataFrame, window_hours: int) -> pd.DataFrame:
    """ad별로 (now, past=now-Wh ±1h) 페어 만들어 델타 샘플."""
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
                "AD_ID": ad_id,
                "AD_NAME": now["AD_NAME"],
                "CAMPAIGN_NAME": now["CAMPAIGN_NAME"],
                "ADSET_NAME": now["ADSET_NAME"],
                "NOW_TS": now["SNAPSHOT_TS"],
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


def pct(s: pd.Series, label: str, pcts=(50, 75, 80, 90, 95, 99)) -> str:
    s = s.dropna()
    if len(s) == 0:
        return f"  {label}: 데이터 없음"
    body = ", ".join(f"p{q}={s.quantile(q/100):.2f}" for q in pcts)
    return f"  {label}: n={len(s)}, mean={s.mean():.2f}, max={s.max():.2f}\n    {body}"


def daily_hits(samples: pd.DataFrame, mask: pd.Series) -> float:
    """일평균 hit 수 (중복 ad_id 6h 쿨다운 무시한 raw)."""
    if not mask.any():
        return 0.0
    hit = samples[mask]
    days_span = max(1.0, (samples["NOW_TS"].max() - samples["NOW_TS"].min()).total_seconds() / 86400)
    return len(hit) / days_span


def daily_unique_ads(samples: pd.DataFrame, mask: pd.Series) -> float:
    """일평균 unique ad 수 — 6h 쿨다운 적용시 실제 알럿 수에 더 근사."""
    if not mask.any():
        return 0.0
    hit = samples[mask].copy()
    days_span = max(1.0, (samples["NOW_TS"].max() - samples["NOW_TS"].min()).total_seconds() / 86400)
    # ad_id × day-of-half (6h 쿨다운 근사)
    hit["BUCKET"] = hit["NOW_TS"].dt.floor("6h")
    return hit.drop_duplicates(["AD_ID", "BUCKET"]).shape[0] / days_span


def analyze_perf(brand: str, label: str):
    print(f"\n{'='*80}\n  [{label}] Performance (Non-BR) — 7일\n{'='*80}")
    raw = fetch_recent(brand, DAYS)
    if raw.empty:
        print("  데이터 없음"); return
    raw["SNAPSHOT_TS"] = pd.to_datetime(raw["SNAPSHOT_TS"], utc=True)
    print(f"  rows={len(raw)}, ads={raw['AD_ID'].nunique()}, "
          f"snap range={raw['SNAPSHOT_TS'].min()} ~ {raw['SNAPSHOT_TS'].max()}")

    s6  = build_rolling(raw, 6)
    s12 = build_rolling(raw, 12)
    p6  = s6[~s6["IS_BR"]].copy()
    p12 = s12[~s12["IS_BR"]].copy()
    if p6.empty:
        print("  Performance 샘플 없음"); return

    print(f"\n  rolling 6h: {len(p6)} samples / {p6['AD_ID'].nunique()} ads")
    print(f"  rolling 12h: {len(p12)} samples / {p12['AD_ID'].nunique()} ads")

    print("\n[6h 분포]")
    for col, lab in [("SPEND","SPEND_6H(원)"),("PURCH","PURCH_6H(건)"),
                     ("ROAS","ROAS_6H(배)"),("CLK","CLK_6H"),("IMP","IMP_6H")]:
        print(pct(p6[col], lab))

    # purch>=N별 분포
    print("\n[purch_6h>=N 코호트]")
    for n in [1,2,3,4,5]:
        sub = p6[p6["PURCH"] >= n]
        if len(sub) == 0:
            print(f"  purch>={n}: 0개"); continue
        print(f"  purch>={n}: n={len(sub)}, "
              f"spend(p50={sub['SPEND'].quantile(.5):.0f}, p75={sub['SPEND'].quantile(.75):.0f}), "
              f"roas(p50={sub['ROAS'].quantile(.5):.2f}, p75={sub['ROAS'].quantile(.75):.2f}), "
              f"ads={sub['AD_ID'].nunique()}")

    # 12h trend gate 시뮬 (roas_6h >= roas_12h 조건 효과)
    merged = p6.merge(
        p12[["AD_ID","NOW_TS","ROAS","SPEND","PURCH"]].rename(
            columns={"ROAS":"ROAS12","SPEND":"SPEND12","PURCH":"PURCH12"}),
        on=["AD_ID","NOW_TS"], how="left")

    # 진입게이트 그리드 (균형 타깃)
    print("\n[진입게이트 그리드 — 6h 조건만, 일평균 hit 추정]")
    print(f"  {'rule':<46} {'hits':>5} {'ads/d':>6} {'samp/d':>7}")
    for roas in [3.0, 3.5, 4.0, 4.5, 5.0]:
        for spend in [5000, 7500, 10000, 15000]:
            for purch in [2, 3, 4]:
                mask = (p6["ROAS"] >= roas) & (p6["SPEND"] >= spend) & (p6["PURCH"] >= purch)
                if not mask.any():
                    continue
                rule = f"roas>={roas} spend>={spend:,} purch>={purch}"
                print(f"  {rule:<46} {mask.sum():>5} {daily_unique_ads(p6, mask):>6.1f} {daily_hits(p6, mask):>7.1f}")

    # 12h 추세 게이트 결합 (roas_6h >= roas_12h)
    print("\n[+12h 추세 게이트 (roas_6h >= roas_12h) 결합]")
    print(f"  {'rule':<46} {'hits':>5} {'ads/d':>6}")
    base_grid = [(4.0,10000,3),(3.5,10000,3),(3.5,7500,2),(4.0,7500,2),(5.0,5000,2),(4.5,5000,2)]
    for roas, spend, purch in base_grid:
        mask = (merged["ROAS"] >= roas) & (merged["SPEND"] >= spend) & (merged["PURCH"] >= purch)
        mask &= (merged["ROAS"].fillna(0) >= merged["ROAS12"].fillna(0))
        rule = f"roas>={roas} spend>={spend:,} purch>={purch} +trend"
        print(f"  {rule:<46} {mask.sum():>5} {daily_unique_ads(merged, mask):>6.1f}")

    # ACTION_CONDITIONS 후보 (각 분기)
    print("\n[ACTION_CONDITIONS 후보 (진입 통과분만)]")
    # 진입게이트 (현행 v1 기준)
    if brand == "MLB":
        entry = (p6["ROAS"]>=4.0)&(p6["SPEND"]>=10000)&(p6["PURCH"]>=3)
    else:
        entry = (p6["ROAS"]>=5.0)&(p6["SPEND"]>=5000)&(p6["PURCH"]>=2)
    eligible = p6[entry]
    print(f"  진입통과 샘플={len(eligible)}, ads={eligible['AD_ID'].nunique() if len(eligible) else 0}")
    if len(eligible) > 0:
        for label2, m2 in [
            ("CAMPAIGN_SCALE(purch>=4)",   eligible[eligible["PURCH"]>=4]),
            ("CAMPAIGN_SCALE(purch>=5)",   eligible[eligible["PURCH"]>=5]),
            ("PRODUCT_EXTRACT(spend>=50k)",eligible[eligible["SPEND"]>=50000]),
            ("PRODUCT_EXTRACT(spend>=30k)",eligible[eligible["SPEND"]>=30000]),
            ("PRODUCT_EXTRACT(spend>=25k)",eligible[eligible["SPEND"]>=25000]),
            ("CREATIVE_EXPAND(purch>=3)",  eligible[eligible["PURCH"]>=3]),
            ("CREATIVE_EXPAND(purch>=2)",  eligible[eligible["PURCH"]>=2]),
        ]:
            print(f"    {label2:<32} n={len(m2):>4}, ads={m2['AD_ID'].nunique():>3}")

    return s6, s12


def analyze_br(brand: str, label: str):
    print(f"\n{'='*80}\n  [{label}] BR (브랜딩) — 7일\n{'='*80}")
    raw = fetch_recent(brand, DAYS)
    if raw.empty:
        print("  데이터 없음"); return
    s6  = build_rolling(raw, 6)
    s12 = build_rolling(raw, 12)
    b6  = s6[s6["IS_BR"]].copy()
    b12 = s12[s12["IS_BR"]].copy()
    if b6.empty:
        print("  BR 샘플 없음"); return

    print(f"  6h: {len(b6)} samples / {b6['AD_ID'].nunique()} ads")
    print(f"  12h: {len(b12)} samples / {b12['AD_ID'].nunique()} ads")

    print("\n[6h 분포]")
    for col, lab in [("IMP","IMP_6H"),("CLK","CLK_6H"),("CTR","CTR_6H"),("SPEND","SPEND_6H")]:
        print(pct(b6[col], lab))

    # CTR_6h vs CTR_12h ratio
    merged = b6.merge(
        b12[["AD_ID","NOW_TS","CTR","CLK","IMP"]].rename(columns={"CTR":"CTR12","CLK":"CLK12","IMP":"IMP12"}),
        on=["AD_ID","NOW_TS"], how="left")
    merged["RATIO"] = np.where(merged["CTR12"]>0, merged["CTR"]/merged["CTR12"], np.nan)
    print("\n[CTR_6h / CTR_12h ratio 분포]")
    print(pct(merged["RATIO"], "RATIO"))

    print("\n[BR 그리드 — entry imp/clk 통과분 중 ratio별 hit]")
    base = merged[(merged["IMP"]>=1000)&(merged["CLK"]>=30)&(merged["CLK12"].fillna(0)>=50)&(merged["CTR12"].fillna(0)>=0.02)]
    print(f"  base (imp>=1k, clk6>=30, clk12>=50, ctr12>=2%): n={len(base)}, ads={base['AD_ID'].nunique()}")
    for r_up, r_dn in [(1.10,0.80),(1.10,0.85),(1.15,0.85),(1.20,0.80),(1.20,0.85),(1.30,0.85)]:
        up = base[base["RATIO"]>=r_up]
        dn = base[base["RATIO"]<=r_dn]
        print(f"    surge>={r_up}/drop<={r_dn}: up n={len(up)} ads={up['AD_ID'].nunique()} / "
              f"dn n={len(dn)} ads={dn['AD_ID'].nunique()} → ads/d≈{(up['AD_ID'].nunique()+dn['AD_ID'].nunique())/DAYS:.1f}")


def analyze_kill(brand: str, label: str):
    print(f"\n{'='*80}\n  [{label}] Kill Alert — 7일\n{'='*80}")
    raw = fetch_recent(brand, DAYS)
    if raw.empty:
        print("  데이터 없음"); return
    s12 = build_rolling(raw, 12)
    k = s12[~s12["IS_BR"]].copy()
    k = k[k["SPEND"] >= 50000]   # 의미 있는 spend 모수
    if k.empty:
        print("  유의 샘플 없음"); return
    print(f"  spend_12h>=50k 모수: n={len(k)}, ads={k['AD_ID'].nunique()}")
    print(pct(k["ROAS"], "ROAS_12H"))
    print(pct(k["SPEND"], "SPEND_12H"))

    print("\n[Kill 그리드 — 일평균 hit]")
    for roas_max in [0.8, 1.0, 1.2, 1.5]:
        for spend_min in [50000, 100000, 150000, 200000]:
            mask = (k["ROAS"]<=roas_max) & (k["SPEND"]>=spend_min)
            if not mask.any():
                continue
            ads_d = daily_unique_ads(k, mask)
            print(f"  roas<={roas_max} spend>={spend_min:,}: n={mask.sum()}, ads/d≈{ads_d:.1f}")


def main():
    kst = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M KST")
    print(f"[분석 시각] {kst}  /  최근 {DAYS}일")
    print(f"[Snowflake] {TABLE}")

    analyze_perf("MLB",      "MLB 성인")
    analyze_perf("MLB_KIDS", "MLB 키즈")
    analyze_br  ("MLB",      "MLB 성인 BR")
    analyze_br  ("MLB_KIDS", "MLB 키즈 BR")
    analyze_kill("MLB",      "MLB 성인")
    analyze_kill("MLB_KIDS", "MLB 키즈")


if __name__ == "__main__":
    main()
