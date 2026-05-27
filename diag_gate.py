"""trend_gate까지 적용한 5/19~5/27 실제 hits 산출.
abs_gate: roas_6h>=5, spend_6h>=10k, purch_6h>=3 (성인/키즈 동일)
trend_gate: spend_prev_6h>=10k AND roas_6h>roas_prev_6h
=> opp_gate = abs_gate AND trend_gate (production과 동일)

추가로 v1 기준(roas>=4 purch>=2)도 abs/abs+trend 양쪽 산출 — 임계치 영향 분리.
"""
import os, sys
from dotenv import load_dotenv
load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
with open(key_path, "rb") as f:
    p_key = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    private_key=pkb,
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE"),
)
TABLE = os.getenv("SNOWFLAKE_TABLE", "META_AD_SNAPSHOT").upper()
cur = conn.cursor()

q = f"""
WITH base AS (
  SELECT ad_id, brand, snapshot_ts,
         CONVERT_TIMEZONE('UTC','Asia/Seoul', snapshot_ts) AS kst_ts,
         TO_DATE(CONVERT_TIMEZONE('UTC','Asia/Seoul', snapshot_ts)) AS kst_date,
         COALESCE(spend_total_cum,     spend_cum)     AS sp,
         COALESCE(purchases_total_cum, purchases_cum) AS pu,
         COALESCE(revenue_total_cum,   revenue_cum)   AS rv
  FROM {TABLE}
  WHERE brand LIKE 'MLB%' AND snapshot_ts >= '2026-05-18'
),
/* 매 시각 n 에 대해 6h 전·12h 전 가장 가까운 row를 잡아 델타 계산 */
nearest AS (
  SELECT n.kst_date, n.kst_ts, n.brand, n.ad_id, n.sp AS sp_now, n.pu AS pu_now, n.rv AS rv_now,
         (SELECT MAX(snapshot_ts) FROM base p6
            WHERE p6.ad_id = n.ad_id
              AND p6.snapshot_ts BETWEEN DATEADD('hour', -8, n.snapshot_ts)
                                     AND DATEADD('hour', -4, n.snapshot_ts)) AS ts_6h,
         (SELECT MAX(snapshot_ts) FROM base p12
            WHERE p12.ad_id = n.ad_id
              AND p12.snapshot_ts BETWEEN DATEADD('hour', -14, n.snapshot_ts)
                                      AND DATEADD('hour', -10, n.snapshot_ts)) AS ts_12h
  FROM base n
),
joined AS (
  SELECT nr.kst_date, nr.kst_ts, nr.brand, nr.ad_id,
         (nr.sp_now - b6.sp)  AS spend_6h,
         (nr.pu_now - b6.pu)  AS purch_6h,
         (nr.rv_now - b6.rv)  AS rev_6h,
         CASE WHEN (nr.sp_now - b6.sp) > 0 THEN (nr.rv_now - b6.rv) / (nr.sp_now - b6.sp) ELSE 0 END AS roas_6h,
         (nr.sp_now - b12.sp) AS spend_12h,
         (nr.pu_now - b12.pu) AS purch_12h,
         (nr.rv_now - b12.rv) AS rev_12h
  FROM nearest nr
  JOIN base b6  ON nr.ad_id = b6.ad_id  AND nr.ts_6h  = b6.snapshot_ts
  JOIN base b12 ON nr.ad_id = b12.ad_id AND nr.ts_12h = b12.snapshot_ts
),
deltas AS (
  SELECT kst_date, kst_ts, brand, ad_id,
         spend_6h, purch_6h, roas_6h,
         GREATEST(spend_12h - spend_6h, 0) AS spend_prev_6h,
         GREATEST(purch_12h - purch_6h, 0) AS purch_prev_6h,
         GREATEST(rev_12h   - rev_6h,   0) AS rev_prev_6h,
         CASE WHEN (spend_12h - spend_6h) > 0
              THEN (rev_12h - rev_6h) / (spend_12h - spend_6h) ELSE 0 END AS roas_prev_6h
  FROM joined
),
hourly AS (
  SELECT kst_date, brand, ad_id,
         MAX(CASE WHEN roas_6h>=5 AND purch_6h>=3 AND spend_6h>=10000 THEN 1 ELSE 0 END) AS abs_v2,
         MAX(CASE WHEN roas_6h>=5 AND purch_6h>=3 AND spend_6h>=10000
                       AND spend_prev_6h>=10000 AND roas_6h>roas_prev_6h THEN 1 ELSE 0 END) AS gate_v2,
         MAX(CASE WHEN brand='MLB'      AND roas_6h>=4 AND purch_6h>=2 AND spend_6h>=10000 THEN 1
                  WHEN brand='MLB_KIDS' AND roas_6h>=4 AND purch_6h>=2 AND spend_6h>=5000  THEN 1
                  ELSE 0 END) AS abs_v1,
         MAX(CASE WHEN brand='MLB'      AND roas_6h>=4 AND purch_6h>=2 AND spend_6h>=10000
                                        AND spend_prev_6h>=10000 AND roas_6h>roas_prev_6h THEN 1
                  WHEN brand='MLB_KIDS' AND roas_6h>=4 AND purch_6h>=2 AND spend_6h>=5000
                                        AND spend_prev_6h>=5000  AND roas_6h>roas_prev_6h THEN 1
                  ELSE 0 END) AS gate_v1
  FROM deltas
  GROUP BY 1, 2, 3
)
SELECT kst_date, brand,
       COUNT(DISTINCT ad_id) AS ads,
       SUM(abs_v2)  AS abs_v2,
       SUM(gate_v2) AS gate_v2,
       SUM(abs_v1)  AS abs_v1,
       SUM(gate_v1) AS gate_v1
FROM hourly
GROUP BY 1, 2
ORDER BY 1, 2
"""
cur.execute(q)
print("=" * 95)
print("[v2 = 현행, v1 = 5/19 당시] abs=절대게이트만, gate=abs+trend(spend_prev_6h>=imp & roas_6h>roas_prev_6h)")
print("=" * 95)
print(f"  {'date':10}  {'brand':9}  {'ads':4}  {'abs_v2':6}  {'gate_v2':7}  {'abs_v1':6}  {'gate_v1':7}")
for r in cur.fetchall():
    print(f"  {r[0]}  {r[1]:9}  {r[2]:4d}  {r[3]:6d}  {r[4]:7d}  {r[5]:6d}  {r[6]:7d}")

cur.close()
conn.close()
