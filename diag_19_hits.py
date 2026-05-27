"""19~26일 일별/시간별 알럿 후보 hits 추정.
- 매 시간 스냅샷에서 6h 전 스냅샷 cum 차이로 델타 계산
- 현행 v2 임계치: roas_6h>=5, purch_6h>=3, spend_6h>=10000 (성인/키즈 동일)
- BR 임계치: clk_6h>=30, imp_6h>=1000, ctr_12h>=0.02, ratio>=1.10 (surge) / <=0.80~0.85 (drop)
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

print("=" * 76)
print("[A] 일별 Performance hits (현행 v2: roas>=5, purch>=3, spend>=10k)")
print("[A] 비교를 위해 v1 (성인 roas>=4 purch>=2 / 키즈 spend>=5k purch>=2) hits도 계산")
print("=" * 76)
q = f"""
WITH base AS (
  SELECT ad_id, brand, snapshot_ts,
         CONVERT_TIMEZONE('UTC','Asia/Seoul', snapshot_ts) AS kst_ts,
         TO_DATE(CONVERT_TIMEZONE('UTC','Asia/Seoul', snapshot_ts)) AS kst_date,
         COALESCE(spend_total_cum, spend_cum) AS sp,
         COALESCE(purchases_total_cum, purchases_cum) AS pu,
         COALESCE(revenue_total_cum, revenue_cum) AS rv
  FROM {TABLE}
  WHERE brand LIKE 'MLB%'
    AND snapshot_ts >= '2026-05-18'
),
deltas AS (
  SELECT n.kst_date, n.kst_ts, n.brand, n.ad_id,
         (n.sp - p.sp) AS spend_6h,
         (n.pu - p.pu) AS purch_6h,
         (n.rv - p.rv) AS rev_6h,
         CASE WHEN (n.sp - p.sp) > 0 THEN (n.rv - p.rv) / (n.sp - p.sp) ELSE 0 END AS roas_6h
  FROM base n
  JOIN base p
    ON n.ad_id = p.ad_id
   AND p.snapshot_ts BETWEEN DATEADD('hour', -8, n.snapshot_ts) AND DATEADD('hour', -4, n.snapshot_ts)
   AND p.snapshot_ts < n.snapshot_ts
),
hourly AS (
  SELECT kst_date, brand, ad_id,
         MAX(roas_6h) AS roas_6h_max,
         MAX(purch_6h) AS purch_6h_max,
         MAX(spend_6h) AS spend_6h_max
  FROM deltas
  GROUP BY 1, 2, 3
)
SELECT kst_date, brand,
       COUNT(DISTINCT ad_id) AS ads,
       SUM(CASE WHEN roas_6h_max>=5 AND purch_6h_max>=3 AND spend_6h_max>=10000 THEN 1 ELSE 0 END) AS hits_v2,
       SUM(CASE WHEN brand='MLB'      AND roas_6h_max>=4 AND purch_6h_max>=2 AND spend_6h_max>=10000 THEN 1
                WHEN brand='MLB_KIDS' AND roas_6h_max>=4 AND purch_6h_max>=2 AND spend_6h_max>=5000  THEN 1
                ELSE 0 END) AS hits_v1
FROM hourly
GROUP BY 1, 2
ORDER BY 1, 2
"""
cur.execute(q)
for r in cur.fetchall():
    print(f"  {r[0]}  {r[1]:9}  ads={r[2]:4d}  hits_v2={r[3]:3d}  hits_v1={r[4]:3d}")

print("\n" + "=" * 76)
print("[B] 19~26일 BR hits (v5: clk>=30, surge ratio>=1.10, drop<=0.80/0.85, ctr12h>=2%)")
print("=" * 76)
q_br = f"""
WITH base AS (
  SELECT ad_id, brand, campaign_name, snapshot_ts,
         CONVERT_TIMEZONE('UTC','Asia/Seoul', snapshot_ts) AS kst_ts,
         TO_DATE(CONVERT_TIMEZONE('UTC','Asia/Seoul', snapshot_ts)) AS kst_date,
         COALESCE(clicks_total_cum,      clicks_cum)      AS cl,
         COALESCE(impressions_total_cum, impressions_cum) AS im
  FROM {TABLE}
  WHERE brand LIKE 'MLB%'
    AND snapshot_ts >= '2026-05-18'
    AND REGEXP_LIKE(UPPER(campaign_name), '(^|[ _/|\\\\-])BR([ _/|\\\\-]|$)')
),
deltas AS (
  SELECT n.kst_date, n.brand, n.ad_id,
         (n.cl - p6.cl)  AS clk_6h,
         (n.im - p6.im)  AS imp_6h,
         (n.cl - p12.cl) AS clk_12h,
         (n.im - p12.im) AS imp_12h,
         CASE WHEN (n.im - p6.im) > 0  THEN (n.cl - p6.cl)*1.0  / (n.im - p6.im)  ELSE 0 END AS ctr_6h,
         CASE WHEN (n.im - p12.im) > 0 THEN (n.cl - p12.cl)*1.0 / (n.im - p12.im) ELSE 0 END AS ctr_12h
  FROM base n
  JOIN base p6
    ON n.ad_id = p6.ad_id
   AND p6.snapshot_ts BETWEEN DATEADD('hour', -8, n.snapshot_ts) AND DATEADD('hour', -4, n.snapshot_ts)
   AND p6.snapshot_ts < n.snapshot_ts
  JOIN base p12
    ON n.ad_id = p12.ad_id
   AND p12.snapshot_ts BETWEEN DATEADD('hour', -14, n.snapshot_ts) AND DATEADD('hour', -10, n.snapshot_ts)
   AND p12.snapshot_ts < n.snapshot_ts
),
day_best AS (
  SELECT kst_date, brand, ad_id,
         MAX(CASE WHEN clk_6h>=30 AND imp_6h>=1000 AND ctr_12h>=0.02
                       AND ctr_6h >= ctr_12h*1.10 AND clk_12h>=50 THEN 1 ELSE 0 END) AS surge_hit,
         MAX(CASE WHEN clk_6h>=30 AND imp_6h>=1000 AND ctr_12h>=0.02
                       AND ((brand='MLB' AND ctr_6h <= ctr_12h*0.80) OR (brand='MLB_KIDS' AND ctr_6h <= ctr_12h*0.85))
                       AND clk_12h>=50 THEN 1 ELSE 0 END) AS drop_hit
  FROM deltas
  GROUP BY 1, 2, 3
)
SELECT kst_date, brand,
       COUNT(DISTINCT ad_id) AS br_ads,
       SUM(surge_hit) AS surge_hits,
       SUM(drop_hit)  AS drop_hits
FROM day_best
GROUP BY 1, 2
ORDER BY 1, 2
"""
cur.execute(q_br)
for r in cur.fetchall():
    print(f"  {r[0]}  {r[1]:9}  br_ads={r[2]:3d}  surge={r[3]:3d}  drop={r[4]:3d}")

print("\n" + "=" * 76)
print("[C] 가장 최근 스냅샷 + 24~26일 시간별 누락 진단")
print("=" * 76)
cur.execute(f"""
  SELECT brand,
         MAX(snapshot_ts) AS last_utc,
         MAX(CONVERT_TIMEZONE('UTC','Asia/Seoul', snapshot_ts)) AS last_kst
  FROM {TABLE}
  WHERE brand LIKE 'MLB%'
  GROUP BY 1
""")
for r in cur.fetchall():
    print(f"  {r[0]:9}  last_utc={r[1]}  last_kst={r[2]}")

cur.close()
conn.close()
