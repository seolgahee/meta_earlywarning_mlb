"""
BR 알럿 발송 검증 — 최근 N시간 동안 BR_CTR_SURGE/DROP 진입 조건을 통과한 광고를
시점별로 dump. 슬랙에 받은 BR 알럿 본문과 ad_name + ctr_6h/ctr_12h/ratio 값이
일치하는지 수동 매칭 검증용.

usage:
    python verify_br_hits.py            # 최근 24시간
    python verify_br_hits.py 48         # 최근 48시간
"""
import os
import sys
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
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "role": os.getenv("SNOWFLAKE_ROLE", "PU_PF"),
}
TABLE = f"{SF['database']}.{SF['schema']}.{os.getenv('SNOWFLAKE_TABLE', 'META_AD_SNAPSHOT')}"

# app.py:206 BR_ALERT_CONDITIONS_BY_BRAND v3 와 동일하게 유지
BR_COND = {
    "ADULT": dict(imp=1_000, clk=30, clk_12h=50, surge=1.20, drop=0.85),
    "KIDS":  dict(imp=1_000, clk=20, clk_12h=50, surge=1.15, drop=0.85),
}


def _pk():
    with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), "rb") as f:
        key = load_pem_private_key(f.read(), password=None, backend=default_backend())
    return key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())


def fetch_recent(hours: int) -> pd.DataFrame:
    sql = f"""
    SELECT brand, ad_id, ad_name, campaign_name, snapshot_ts,
           impressions_cum, clicks_cum
    FROM {TABLE}
    WHERE brand IN ('MLB', 'MLB_KIDS')
      AND snapshot_ts >= DATEADD('hour', -{hours + 14},
                                 CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))
    ORDER BY ad_id, snapshot_ts
    """
    conn = snowflake.connector.connect(private_key=_pk(), **SF)
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    df.columns = [c.upper() for c in df.columns]
    df["SNAPSHOT_TS"] = pd.to_datetime(df["SNAPSHOT_TS"], utc=True)
    # 동일 (ad_id, snapshot_ts) 가 채널별로 중복 적재되는 경우 dedup — 누적값은 동일하므로 first 채택
    df = df.drop_duplicates(subset=["AD_ID", "SNAPSHOT_TS"], keep="first")
    return df


def compute_rolling(df: pd.DataFrame, window_hours: int) -> pd.DataFrame:
    """app.py:2052 evaluate_alerts 와 동일 로직: 각 (ad, now)에 대해 -window_hours
    시점에서 가장 가까운 스냅샷(±2h)을 찾아 누적값 차이로 델타 계산."""
    df = df.sort_values(["AD_ID", "SNAPSHOT_TS"]).copy()
    rows = []
    for ad_id, g in df.groupby("AD_ID"):
        snaps = g.to_dict("records")
        ts_list = [r["SNAPSHOT_TS"] for r in snaps]
        for i, now in enumerate(snaps):
            target = now["SNAPSHOT_TS"] - pd.Timedelta(hours=window_hours)
            diffs = [(abs((ts - target).total_seconds()), j)
                     for j, ts in enumerate(ts_list)
                     if j != i and abs((ts - target).total_seconds()) <= 7200]
            if not diffs:
                continue
            _, j = min(diffs)
            past = snaps[j]
            if past["SNAPSHOT_TS"] >= now["SNAPSHOT_TS"]:
                continue
            imp = max(0, (now["IMPRESSIONS_CUM"] or 0) - (past["IMPRESSIONS_CUM"] or 0))
            clk = max(0, (now["CLICKS_CUM"] or 0) - (past["CLICKS_CUM"] or 0))
            rows.append({
                "AD_ID": ad_id, "AD_NAME": now["AD_NAME"], "BRAND": now["BRAND"],
                "CAMPAIGN_NAME": now["CAMPAIGN_NAME"], "NOW_TS": now["SNAPSHOT_TS"],
                "IMP": imp, "CLK": clk,
                "CTR": (clk / imp) if imp > 0 else np.nan,
            })
    return pd.DataFrame(rows)


def is_br(name: str) -> bool:
    import re
    return "BR" in [t.upper() for t in re.split(r"[\s_\-|/]+", name or "")]


def main():
    hours = int(sys.argv[1]) if len(sys.argv) > 1 else 24
    print(f"[검증] 최근 {hours}시간 BR 알럿 진입 hits dump")
    print(f"[Snowflake] {TABLE}")

    raw = fetch_recent(hours)
    if raw.empty:
        print("데이터 없음"); return
    print(f"  raw rows={len(raw)}, ads={raw['AD_ID'].nunique()}, "
          f"snaps={raw['SNAPSHOT_TS'].nunique()}, "
          f"range={raw['SNAPSHOT_TS'].min()} ~ {raw['SNAPSHOT_TS'].max()}")

    s6  = compute_rolling(raw, 6)
    s12 = compute_rolling(raw, 12)

    # 최근 N시간 내 NOW_TS 만 필터
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(hours=hours)
    s6  = s6[s6["NOW_TS"]  >= cutoff].copy()
    s12 = s12[s12["NOW_TS"] >= cutoff].copy()

    # 6h + 12h merge
    m = s6.merge(
        s12[["AD_ID", "NOW_TS", "IMP", "CLK", "CTR"]].rename(
            columns={"IMP": "IMP_12H", "CLK": "CLK_12H", "CTR": "CTR_12H"}
        ),
        on=["AD_ID", "NOW_TS"], how="inner",
    )
    m["IS_BR"] = m["CAMPAIGN_NAME"].apply(is_br)
    m = m[m["IS_BR"]].copy()
    m["SEG"] = np.where(m["BRAND"] == "MLB_KIDS", "KIDS", "ADULT")
    m["RATIO"] = np.where(m["CTR_12H"] > 0, m["CTR"] / m["CTR_12H"], np.nan)
    m["NOW_KST"] = m["NOW_TS"].dt.tz_convert("Asia/Seoul").dt.strftime("%m-%d %H:%M")

    for seg, cond in BR_COND.items():
        sub = m[m["SEG"] == seg].copy()
        entry = (sub["IMP"] >= cond["imp"]) & (sub["CLK"] >= cond["clk"]) & (sub["CLK_12H"] >= cond["clk_12h"])
        surge = sub[entry & (sub["RATIO"] >= cond["surge"])].copy()
        drop  = sub[entry & (sub["RATIO"] <= cond["drop"])].copy()

        print(f"\n{'═' * 80}")
        print(f"  [{seg}] BR 진입 조건: IMP≥{cond['imp']:,} & CLK≥{cond['clk']} & CLK_12H≥{cond['clk_12h']}")
        print(f"         SURGE: ratio≥{cond['surge']}  /  DROP: ratio≤{cond['drop']}")
        print(f"         진입 통과 샘플 {entry.sum()} / 전체 {len(sub)}, SURGE {len(surge)}건 / DROP {len(drop)}건")
        print(f"{'═' * 80}")

        for label, hit in [("SURGE", surge), ("DROP", drop)]:
            if hit.empty:
                print(f"  [{label}] 해당 시간대 hit 없음")
                continue
            print(f"\n  ── [{label}] hits ──")
            hit = hit.sort_values("NOW_TS")
            for _, r in hit.iterrows():
                print(f"  {r['NOW_KST']} KST | ad={r['AD_NAME'][:55]:<55} | "
                      f"imp_6h={int(r['IMP']):>5,} clk_6h={int(r['CLK']):>4} "
                      f"clk_12h={int(r['CLK_12H']):>4} | "
                      f"ctr_6h={r['CTR']:.2%} ctr_12h={r['CTR_12H']:.2%} ratio={r['RATIO']:.3f}")


if __name__ == "__main__":
    main()
