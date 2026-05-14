"""
성인 BR v2 임계치(imp>=1000, clk>=30, ratio>=1.15) 실제 hit 케이스 상세 점검.
- 각 hit 행의 ad_name, imp_6h, clk_6h, ctr_6h, ctr_12h, ratio, imp_12h, clk_12h
- 표본 충분성과 ratio 신뢰도 판별
"""
import os, sys
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
pd.set_option('display.width', 200)
pd.set_option('display.max_columns', 30)
pd.set_option('display.max_rows', 100)

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


import re as _re
BR_RE = _re.compile(r"[\s_\-|/]+")


def is_br(name: str) -> bool:
    return "BR" in [t.upper() for t in BR_RE.split(name or "")]


def main():
    sql = f"""
    SELECT brand, ad_id, ad_name, campaign_name, snapshot_ts,
           impressions_cum, clicks_cum
    FROM {TABLE}
    WHERE brand = 'MLB'
    ORDER BY ad_id, snapshot_ts
    """
    df = q(sql)
    df.columns = [c.upper() for c in df.columns]
    df["SNAPSHOT_TS"] = pd.to_datetime(df["SNAPSHOT_TS"], utc=True)
    df = df[df["CAMPAIGN_NAME"].apply(is_br)].copy()
    print(f"성인 BR raw rows={len(df)}, ads={df['AD_ID'].nunique()}")

    samples = []
    for ad_id, g in df.groupby("AD_ID"):
        rows = g.sort_values("SNAPSHOT_TS").to_dict("records")
        ts_list = [r["SNAPSHOT_TS"] for r in rows]
        for i, now in enumerate(rows):
            for label, hrs in [("6H", 6), ("12H", 12)]:
                target = now["SNAPSHOT_TS"] - pd.Timedelta(hours=hrs)
                diffs = [(abs((ts - target).total_seconds()), j) for j, ts in enumerate(ts_list)]
                diffs = [(d, j) for d, j in diffs if d <= 3600 and j != i]
                if not diffs:
                    continue
                _, j = min(diffs)
                past = rows[j]
                if past["SNAPSHOT_TS"] >= now["SNAPSHOT_TS"]:
                    continue
                if label == "6H":
                    imp_6h = max(0, now["IMPRESSIONS_CUM"] - past["IMPRESSIONS_CUM"])
                    clk_6h = max(0, now["CLICKS_CUM"] - past["CLICKS_CUM"])
                    sample = {"AD_ID": ad_id, "AD_NAME": now["AD_NAME"],
                              "NOW_TS": now["SNAPSHOT_TS"],
                              "IMP_6H": imp_6h, "CLK_6H": clk_6h}
                else:
                    if not samples or samples[-1]["AD_ID"] != ad_id or samples[-1]["NOW_TS"] != now["SNAPSHOT_TS"]:
                        continue
                    samples[-1]["IMP_12H"] = max(0, now["IMPRESSIONS_CUM"] - past["IMPRESSIONS_CUM"])
                    samples[-1]["CLK_12H"] = max(0, now["CLICKS_CUM"] - past["CLICKS_CUM"])
                    continue
                samples.append(sample)

    s = pd.DataFrame(samples).dropna()
    s["CTR_6H"]  = np.where(s["IMP_6H"]  > 0, s["CLK_6H"]  / s["IMP_6H"],  np.nan)
    s["CTR_12H"] = np.where(s["IMP_12H"] > 0, s["CLK_12H"] / s["IMP_12H"], np.nan)
    s["RATIO"]   = np.where(s["CTR_12H"] > 0, s["CTR_6H"]  / s["CTR_12H"], np.nan)

    print(f"전체 (6h+12h 매칭) 샘플 수: {len(s)}")

    print("\n[v2 적용 — imp_6h>=1000 AND clk_6h>=30 AND ratio>=1.15]")
    hit_v2 = s[(s["IMP_6H"] >= 1000) & (s["CLK_6H"] >= 30) & (s["RATIO"] >= 1.15)]
    print(f"hits={len(hit_v2)}, ads={hit_v2['AD_ID'].nunique()}, hours={hit_v2['NOW_TS'].dt.floor('h').nunique()}")
    print(hit_v2[["AD_NAME", "NOW_TS", "IMP_6H", "CLK_6H", "IMP_12H", "CLK_12H",
                   "CTR_6H", "CTR_12H", "RATIO"]].to_string(index=False))

    print("\n[v1 (현행 직전) — imp_6h>=2000 AND clk_6h>=30 AND ratio>=1.05]")
    hit_v1 = s[(s["IMP_6H"] >= 2000) & (s["CLK_6H"] >= 30) & (s["RATIO"] >= 1.05)]
    print(f"hits={len(hit_v1)}, ads={hit_v1['AD_ID'].nunique()}, hours={hit_v1['NOW_TS'].dt.floor('h').nunique()}")
    print(hit_v1[["AD_NAME", "NOW_TS", "IMP_6H", "CLK_6H", "IMP_12H", "CLK_12H",
                   "CTR_6H", "CTR_12H", "RATIO"]].to_string(index=False))

    print("\n[v2 + clk_12h>=50 baseline 가드 시뮬레이션 — 키즈와 동일 가드 적용 시]")
    hit_g = s[(s["IMP_6H"] >= 1000) & (s["CLK_6H"] >= 30) & (s["RATIO"] >= 1.15) & (s["CLK_12H"] >= 50)]
    print(f"hits={len(hit_g)}, ads={hit_g['AD_ID'].nunique()}, hours={hit_g['NOW_TS'].dt.floor('h').nunique()}")
    print(hit_g[["AD_NAME", "NOW_TS", "IMP_6H", "CLK_6H", "IMP_12H", "CLK_12H",
                  "CTR_6H", "CTR_12H", "RATIO"]].to_string(index=False))

    print("\n[참고: 어제 사용자가 받은 키즈 알럿 케이스 — IMP_6H=1051, CLK_6H=22 라인 검증용]")
    print("성인 BR 데이터에서 imp_6h<2000 & clk_6h<50 인 ratio>=1.15 hits:")
    edge = s[(s["IMP_6H"] < 2000) & (s["CLK_6H"] < 50) & (s["RATIO"] >= 1.15)]
    print(edge[["AD_NAME", "NOW_TS", "IMP_6H", "CLK_6H", "IMP_12H", "CLK_12H",
                "CTR_6H", "CTR_12H", "RATIO"]].to_string(index=False))


if __name__ == "__main__":
    main()
