"""
Performance(전환) 알럿 opp_gate 퍼널 진단 — *_CUM(자정버그) vs *_TOTAL_CUM(수정본) 비교.

app.py opp_gate:
  past = (now - Nh) 에 가장 가까운 스냅샷, 윈도우 [now-Nh-2h, now-Nh+2h]
  CUM   모드: delta = cum(now) - cum(past)        (past 없으면 0 → delta=cum 전체)
  TOTAL 모드: delta = total(now) - total(past)    (past 없으면 delta=0)
  roas  = revenue/spend if spend>0 else 0
  prev_6h = (delta_12h - delta_6h).clip(lower=0)
  abs_gate   = roas_6h>=4 AND spend_6h>=10,000 AND purch_6h>=3
  trend_gate = spend_prev_6h>=10,000 AND roas_6h>roas_prev_6h
  opp_gate   = abs_gate AND trend_gate
"""
import os, sys
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key, Encoding, PrivateFormat, NoEncryption,
)

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")

SF = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"), "user": os.getenv("SNOWFLAKE_USER"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"), "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"), "role": os.getenv("SNOWFLAKE_ROLE", "PU_PF"),
}
TABLE = f"{SF['database']}.{SF['schema']}.{os.getenv('SNOWFLAKE_TABLE', 'META_AD_SNAPSHOT')}"
KST = ZoneInfo("Asia/Seoul")


def _pk():
    with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), "rb") as f:
        key = load_pem_private_key(f.read(), password=None, backend=default_backend())
    return key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())


def fetch(brand):
    sql = f"""
    SELECT ad_id, ad_name, campaign_name, snapshot_ts,
           spend_cum, revenue_cum, purchases_cum,
           spend_total_cum, revenue_total_cum, purchases_total_cum
    FROM {TABLE}
    WHERE brand = '{brand}'
    ORDER BY ad_id, snapshot_ts
    """
    conn = snowflake.connector.connect(private_key=_pk(), **SF)
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    df.columns = [c.upper() for c in df.columns]
    df["SNAPSHOT_TS"] = pd.to_datetime(df["SNAPSHOT_TS"], utc=True)
    return df.drop_duplicates(subset=["AD_ID", "SNAPSHOT_TS"], keep="first")


def find_past(ts_list, i, now_ts, hours):
    target = now_ts - pd.Timedelta(hours=hours)
    best = None
    for j, ts in enumerate(ts_list):
        if j == i:
            continue
        d = abs((ts - target).total_seconds())
        if d <= 7200 and (best is None or d < best[0]):
            best = (d, j)
    return best[1] if best else None


def is_br(name):
    import re
    return "BR" in [t.upper() for t in re.split(r"[\s_\-|/]+", name or "")]


def build(raw, mode):
    """mode: 'CUM' or 'TOTAL'. 각 (ad, now) 의 6h/12h 델타·게이트 계산."""
    sc, rc, pc = ("SPEND_CUM", "REVENUE_CUM", "PURCHASES_CUM") if mode == "CUM" else \
                 ("SPEND_TOTAL_CUM", "REVENUE_TOTAL_CUM", "PURCHASES_TOTAL_CUM")
    rows = []
    for ad_id, g in raw.groupby("AD_ID"):
        snaps = g.sort_values("SNAPSHOT_TS").to_dict("records")
        ts_list = [r["SNAPSHOT_TS"] for r in snaps]
        for i, now in enumerate(snaps):
            rec = {"AD_ID": ad_id, "AD_NAME": now["AD_NAME"], "NOW_TS": now["SNAPSHOT_TS"]}
            for hours, lbl in [(6, "6h"), (12, "12h")]:
                j = find_past(ts_list, i, now["SNAPSHOT_TS"], hours)
                if j is None:
                    # CUM: app.py 는 past fillna(0) → delta=cum 전체 / TOTAL: fillna(now) → delta=0
                    rec[f"spend_{lbl}"] = (now[sc] or 0) if mode == "CUM" else 0.0
                    rec[f"rev_{lbl}"]   = (now[rc] or 0) if mode == "CUM" else 0.0
                    rec[f"purch_{lbl}"] = (now[pc] or 0) if mode == "CUM" else 0.0
                else:
                    p = snaps[j]
                    rec[f"spend_{lbl}"] = (now[sc] or 0) - (p[sc] or 0)
                    rec[f"rev_{lbl}"]   = (now[rc] or 0) - (p[rc] or 0)
                    rec[f"purch_{lbl}"] = (now[pc] or 0) - (p[pc] or 0)
            rows.append(rec)
    d = pd.DataFrame(rows)
    d["roas_6h"] = np.where(d["spend_6h"] > 0, d["rev_6h"] / d["spend_6h"], 0.0)
    d["spend_prev_6h"] = (d["spend_12h"] - d["spend_6h"]).clip(lower=0)
    d["rev_prev_6h"]   = (d["rev_12h"]   - d["rev_6h"]).clip(lower=0)
    d["roas_prev_6h"]  = np.where(d["spend_prev_6h"] > 0, d["rev_prev_6h"] / d["spend_prev_6h"], 0.0)
    d["HOUR"] = d["NOW_TS"].dt.tz_convert(KST).dt.hour
    d["abs_gate"] = (d["roas_6h"] >= 4.0) & (d["spend_6h"] >= 10_000) & (d["purch_6h"] >= 3)
    d["opp_gate"] = d["abs_gate"] & (d["spend_prev_6h"] >= 10_000) & (d["roas_6h"] > d["roas_prev_6h"])
    return d


def main():
    brand = sys.argv[1] if len(sys.argv) > 1 else "MLB"
    print(f"[진단] Performance opp_gate — *_CUM vs *_TOTAL_CUM  brand={brand}")
    print(f"[분석 시각] {datetime.now(KST):%Y-%m-%d %H:%M KST}")
    raw = fetch(brand)
    raw = raw[~raw["CAMPAIGN_NAME"].fillna("").apply(is_br)].copy()
    print(f"  Performance 스냅샷 rows={len(raw)}, ads={raw['AD_ID'].nunique()}")

    dc = build(raw, "CUM")
    dt = build(raw, "TOTAL")

    print(f"\n{'='*60}\n  opp_gate 통과: 전체 {len(dc)} 샘플-시각\n{'='*60}")
    print(f"  *_CUM  (자정버그 有): abs {dc['abs_gate'].sum():>4} → opp {dc['opp_gate'].sum():>4}")
    print(f"  *_TOTAL(수정본)     : abs {dt['abs_gate'].sum():>4} → opp {dt['opp_gate'].sum():>4}")

    print(f"\n  [KST 시간대별 opp_gate 통과 — CUM vs TOTAL]")
    print(f"  {'시각':>5} {'샘플':>5} {'CUM_abs':>8} {'CUM_opp':>8} {'TOT_abs':>8} {'TOT_opp':>8}")
    gc = dc.groupby("HOUR").agg(n=("AD_ID", "size"), a=("abs_gate", "sum"), o=("opp_gate", "sum"))
    gt = dt.groupby("HOUR").agg(a=("abs_gate", "sum"), o=("opp_gate", "sum"))
    for h in sorted(gc.index):
        mark = "  ← 자정버그 구간" if (gc.at[h, "o"] == 0 and gt.at[h, "o"] > 0) else ""
        print(f"  {h:02d}시 {int(gc.at[h,'n']):>6} {int(gc.at[h,'a']):>8} {int(gc.at[h,'o']):>8} "
              f"{int(gt.at[h,'a']):>8} {int(gt.at[h,'o']):>8}{mark}")

    am_c = dc[(dc["HOUR"] >= 7) & (dc["HOUR"] <= 13)]["opp_gate"].sum()
    am_t = dt[(dt["HOUR"] >= 7) & (dt["HOUR"] <= 13)]["opp_gate"].sum()
    print(f"\n  오전(07~13시) opp_gate 통과:  CUM={am_c}건  →  TOTAL={am_t}건")


if __name__ == "__main__":
    main()
