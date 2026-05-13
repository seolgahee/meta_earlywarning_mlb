"""BR(브랜딩) 캠페인 임계치 분석 — rolling 6h window.
대상: 캠페인명에 'BR' 토큰 포함 (split by _ - / | whitespace).
지표: impressions_6h, clicks_6h, ctr_6h, ctr_12h, ctr 비율(6h/12h).
"""
import os, sys, re
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
TABLE = f"{SF['database']}.{SF['schema']}.{os.getenv('SNOWFLAKE_TABLE','META_AD_SNAPSHOT')}"

def _pk():
    with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), "rb") as f:
        return load_pem_private_key(f.read(), password=None, backend=default_backend()).private_bytes(
            Encoding.DER, PrivateFormat.PKCS8, NoEncryption()
        )

def qsql(sql):
    c = snowflake.connector.connect(private_key=_pk(), **SF)
    try: return pd.read_sql(sql, c)
    finally: c.close()


def has_br_token(name: str) -> bool:
    if not name: return False
    return "BR" in [t.upper() for t in re.split(r"[\s_\-|/]+", str(name))]


def fetch_all(brand: str) -> pd.DataFrame:
    sql = f"""
    SELECT ad_id, ad_name, campaign_name, snapshot_ts,
           impressions_cum, clicks_cum
    FROM {TABLE}
    WHERE brand = '{brand}'
    ORDER BY ad_id, snapshot_ts
    """
    df = qsql(sql)
    df.columns = [c.upper() for c in df.columns]
    df["IS_BR"] = df["CAMPAIGN_NAME"].apply(has_br_token)
    return df[df["IS_BR"]]


def build_samples(df: pd.DataFrame, window_h: int) -> pd.DataFrame:
    df = df.sort_values(["AD_ID", "SNAPSHOT_TS"]).copy()
    df["SNAPSHOT_TS"] = pd.to_datetime(df["SNAPSHOT_TS"], utc=True)
    out = []
    for ad_id, g in df.groupby("AD_ID"):
        rows = g.to_dict("records")
        ts_list = [r["SNAPSHOT_TS"] for r in rows]
        for i, now in enumerate(rows):
            target = now["SNAPSHOT_TS"] - pd.Timedelta(hours=window_h)
            cands = [(abs((ts - target).total_seconds()), j) for j, ts in enumerate(ts_list)]
            cands = [(d, j) for d, j in cands if d <= 3600 and j != i]
            if not cands: continue
            _, j = min(cands)
            past = rows[j]
            if past["SNAPSHOT_TS"] >= now["SNAPSHOT_TS"]: continue
            out.append({
                "AD_ID": ad_id,
                "CAMPAIGN_NAME": now["CAMPAIGN_NAME"],
                "NOW_TS": now["SNAPSHOT_TS"],
                "IMP": max(0, (now["IMPRESSIONS_CUM"] or 0) - (past["IMPRESSIONS_CUM"] or 0)),
                "CLK": max(0, (now["CLICKS_CUM"] or 0) - (past["CLICKS_CUM"] or 0)),
            })
    s = pd.DataFrame(out)
    if s.empty: return s
    s["CTR"] = np.where(s["IMP"] > 0, s["CLK"] / s["IMP"], np.nan)
    return s


def pct(s: pd.Series, label: str, pcts=(10, 25, 50, 75, 90, 95)):
    s = s.dropna()
    if not len(s):
        print(f"  {label}: no data"); return
    p = {q: s.quantile(q/100) for q in pcts}
    print(f"  {label}: n={len(s)}, mean={s.mean():.4f}, max={s.max():.4f}")
    print(f"    " + ", ".join(f"p{q}={p[q]:.4f}" for q in pcts))


def analyze(brand: str, label: str):
    print(f"\n{'='*80}\n  {label} (brand={brand}) — BR 캠페인 임계치 분석\n{'='*80}")
    raw = fetch_all(brand)
    if raw.empty:
        print("  BR 캠페인 없음"); return
    print(f"  BR raw rows={len(raw)}, 광고={raw['AD_ID'].nunique()}, "
          f"캠페인={raw['CAMPAIGN_NAME'].nunique()}")
    print(f"  캠페인 목록: {sorted(raw['CAMPAIGN_NAME'].unique().tolist())}")

    s6 = build_samples(raw, 6)
    s12 = build_samples(raw, 12)
    print(f"\n  rolling 6h samples={len(s6)}, 12h samples={len(s12)}")

    print("\n[6h 윈도우 분포]")
    pct(s6["IMP"], "IMP_6H")
    pct(s6["CLK"], "CLK_6H")
    pct(s6["CTR"], "CTR_6H")

    # 6h vs 12h CTR 비율 (같은 ad/시점)
    if not s6.empty and not s12.empty:
        merged = s6.merge(s12, on=["AD_ID","NOW_TS"], suffixes=("_6","_12"))
        merged = merged[(merged["CTR_6"].notna()) & (merged["CTR_12"].notna()) & (merged["CTR_12"] > 0)]
        merged["RATIO"] = merged["CTR_6"] / merged["CTR_12"]
        print("\n[CTR_6h / CTR_12h 비율 분포 (같은 ad/now_ts)]")
        pct(merged["RATIO"], "RATIO")
        print(f"\n  ratio>=1.1 (현 문서 SURGE): {(merged['RATIO']>=1.1).sum()}/{len(merged)}")
        print(f"  ratio>1.0  (현 코드 SURGE): {(merged['RATIO']>1.0).sum()}/{len(merged)}")
        print(f"  ratio<=0.9 (현 문서 DROP) : {(merged['RATIO']<=0.9).sum()}/{len(merged)}")
        print(f"  ratio<=0.8 (현 코드 DROP) : {(merged['RATIO']<=0.8).sum()}/{len(merged)}")

    # entry gate 시뮬
    print("\n[Entry gate 시뮬 — imp / clk]")
    for imp_min, clk_min in [(10000, 200), (5000, 100), (3000, 50), (2000, 30), (1000, 20)]:
        m = (s6["IMP"] >= imp_min) & (s6["CLK"] >= clk_min)
        hits = s6[m]
        print(f"  imp>={imp_min:>6}, clk>={clk_min:>4}: {len(hits):>4} 통과 / {len(s6):>4} "
              f"({len(hits)/max(1,len(s6))*100:.1f}%), 광고={hits['AD_ID'].nunique()}")


analyze("MLB", "MLB 성인")
analyze("MLB_KIDS", "MLB 키즈")
