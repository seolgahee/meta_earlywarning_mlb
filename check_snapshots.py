"""스냅샷 누적 상태 확인 — 시간대별, 브랜드별."""
import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key, Encoding, PrivateFormat, NoEncryption,
)

load_dotenv()
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 240)
pd.set_option("display.max_rows", 200)

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


print("=" * 80)
print("[1] 브랜드별 스냅샷 개수 & 시간 범위")
print("=" * 80)
df = q(f"""
SELECT brand,
       COUNT(*) AS row_cnt,
       COUNT(DISTINCT snapshot_ts) AS distinct_snaps,
       COUNT(DISTINCT ad_id) AS distinct_ads,
       MIN(snapshot_ts) AS first_ts,
       MAX(snapshot_ts) AS last_ts,
       DATEDIFF('hour', MIN(snapshot_ts), MAX(snapshot_ts)) AS span_hours
FROM {TABLE}
GROUP BY brand
ORDER BY brand
""")
print(df.to_string(index=False))

print("\n" + "=" * 80)
print("[2] 최근 24h 시간대별 스냅샷 (KST)")
print("=" * 80)
df2 = q(f"""
SELECT brand,
       DATE_TRUNC('hour', CONVERT_TIMEZONE('UTC', 'Asia/Seoul', snapshot_ts)) AS kst_hour,
       COUNT(*) AS row_cnt,
       COUNT(DISTINCT ad_id) AS ads
FROM {TABLE}
WHERE snapshot_ts >= DATEADD('hour', -30, CURRENT_TIMESTAMP())
GROUP BY brand, kst_hour
ORDER BY kst_hour DESC, brand
""")
print(df2.to_string(index=False))

print("\n" + "=" * 80)
print("[3] MLB_KIDS 24h spend/purchases/revenue 추이")
print("=" * 80)
df3 = q(f"""
WITH latest_per_hour AS (
  SELECT brand, ad_id,
         DATE_TRUNC('hour', CONVERT_TIMEZONE('UTC', 'Asia/Seoul', snapshot_ts)) AS kst_hour,
         MAX(snapshot_ts) AS snap_ts,
         MAX(spend_cum) AS spend_cum,
         MAX(purchases_cum) AS purch_cum,
         MAX(revenue_cum) AS rev_cum,
         MAX(clicks_cum) AS clicks_cum,
         MAX(impressions_cum) AS imp_cum
  FROM {TABLE}
  WHERE brand = 'MLB_KIDS'
    AND snapshot_ts >= DATEADD('hour', -30, CURRENT_TIMESTAMP())
  GROUP BY brand, ad_id, kst_hour
)
SELECT kst_hour,
       COUNT(DISTINCT ad_id) AS ads,
       SUM(spend_cum)   AS spend_cum_sum,
       SUM(purch_cum)   AS purch_cum_sum,
       SUM(rev_cum)     AS rev_cum_sum,
       SUM(clicks_cum)  AS clicks_cum_sum,
       SUM(imp_cum)     AS imp_cum_sum
FROM latest_per_hour
GROUP BY kst_hour
ORDER BY kst_hour DESC
""")
print(df3.to_string(index=False))

print("\n" + "=" * 80)
print("[4] MLB(성인) 동일")
print("=" * 80)
df4 = q(f"""
WITH latest_per_hour AS (
  SELECT brand, ad_id,
         DATE_TRUNC('hour', CONVERT_TIMEZONE('UTC', 'Asia/Seoul', snapshot_ts)) AS kst_hour,
         MAX(spend_cum) AS spend_cum,
         MAX(purchases_cum) AS purch_cum,
         MAX(revenue_cum) AS rev_cum
  FROM {TABLE}
  WHERE brand = 'MLB'
    AND snapshot_ts >= DATEADD('hour', -30, CURRENT_TIMESTAMP())
  GROUP BY brand, ad_id, kst_hour
)
SELECT kst_hour,
       COUNT(DISTINCT ad_id) AS ads,
       SUM(spend_cum) AS spend_cum_sum,
       SUM(purch_cum) AS purch_cum_sum,
       SUM(rev_cum)   AS rev_cum_sum
FROM latest_per_hour
GROUP BY kst_hour
ORDER BY kst_hour DESC
""")
print(df4.to_string(index=False))
