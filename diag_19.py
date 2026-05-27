"""19일 이후 알럿 미수신 진단:
1) 스냅샷 적재 시계열 (시간별 row 수) — 워크플로우 실행 여부 확인
2) 19~26일 일별 opp_gate 통과 ad 수 추정 (현행 임계치 적용)
3) 동일 기간 BR_gate 통과 추정
"""
import os
from dotenv import load_dotenv
load_dotenv()

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

print("=" * 70)
print("[1] 19~26일 일별/시간별 스냅샷 수 (워크플로우 실행 확인)")
print("=" * 70)
cur.execute(f"""
  SELECT TO_VARCHAR(CONVERT_TIMEZONE('UTC','Asia/Seoul', snapshot_ts), 'YYYY-MM-DD HH24') AS kst_hr,
         brand, COUNT(*) AS rows_cnt, COUNT(DISTINCT ad_id) AS ads
  FROM {TABLE}
  WHERE brand LIKE 'MLB%'
    AND snapshot_ts >= '2026-05-18'
  GROUP BY 1, 2
  ORDER BY 1 DESC, 2
  LIMIT 200
""")
for r in cur.fetchall():
    print(f"  {r[0]}  {r[1]:9}  rows={r[2]:5d}  ads={r[3]:4d}")

print("\n" + "=" * 70)
print("[2] 19~26일 일별 알럿 후보 추정 (델타 계산 → opp_gate 통과)")
print("=" * 70)
# 각 일자에서 가장 최근 스냅샷 기준, 동일 ad의 6h/12h 전 스냅샷 cum 차이로 델타 계산
cur.execute(f"""
WITH base AS (
  SELECT ad_id, brand, snapshot_ts,
         CONVERT_TIMEZONE('UTC','Asia/Seoul', snapshot_ts) AS kst_ts,
         TO_DATE(CONVERT_TIMEZONE('UTC','Asia/Seoul', snapshot_ts)) AS kst_date,
         COALESCE(spend_total_cum, spend_cum) AS sp,
         COALESCE(purchases_total_cum, purchases_cum) AS pu,
         COALESCE(revenue_total_cum, revenue_cum) AS rv,
         COALESCE(clicks_total_cum, clicks_cum) AS cl,
         COALESCE(impressions_total_cum, impressions_cum) AS im
  FROM {TABLE}
  WHERE brand LIKE 'MLB%'
    AND snapshot_ts >= '2026-05-18'
),
self_join AS (
  SELECT n.kst_date, n.brand, n.ad_id,
         (n.sp - p6.sp) AS spend_6h,
         (n.pu - p6.pu) AS purch_6h,
         (n.rv - p6.rv) AS rev_6h,
         CASE WHEN (n.sp - p6.sp) > 0 THEN (n.rv - p6.rv) / (n.sp - p6.sp) ELSE 0 END AS roas_6h,
         ROW_NUMBER() OVER (PARTITION BY n.kst_date, n.brand, n.ad_id ORDER BY n.kst_ts DESC) AS rn_day
  FROM base n
  JOIN base p6
    ON n.ad_id = p6.ad_id
   AND p6.snapshot_ts BETWEEN DATEADD('hour', -8, n.snapshot_ts) AND DATEADD('hour', -4, n.snapshot_ts)
),
day_best AS (
  SELECT kst_date, brand, ad_id,
         MAX(roas_6h) AS roas_6h_max,
         MAX(purch_6h) AS purch_6h_max,
         MAX(spend_6h) AS spend_6h_max
  FROM self_join
  GROUP BY 1, 2, 3
)
SELECT kst_date, brand,
       SUM(CASE WHEN roas_6h_max >= 5.0 AND purch_6h_max >= 3 AND spend_6h_max >= 10000 THEN 1 ELSE 0 END) AS opp_hits,
       COUNT(DISTINCT ad_id) AS total_ads,
       AVG(roas_6h_max) AS avg_roas
FROM day_best
GROUP BY 1, 2
ORDER BY 1, 2
""")
for r in cur.fetchall():
    print(f"  {r[0]}  {r[1]:9}  opp_hits={r[2]:3d}  total_ads={r[3]:4d}  avg_roas_6h={float(r[4] or 0):.2f}")

print("\n" + "=" * 70)
print("[3] 가장 최근 스냅샷 ts (UTC / KST)")
print("=" * 70)
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
