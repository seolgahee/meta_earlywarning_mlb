"""MLB 스냅샷 적재 시각 분 단위 확인 + 누락 시간대 추적."""
import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key, Encoding, PrivateFormat, NoEncryption,
)

load_dotenv()
pd.set_option("display.max_rows", 200)
pd.set_option("display.width", 200)

SF = dict(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE", "PU_PF"),
)
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
print("[1] MLB/MLB_KIDS 모든 스냅샷 시각 (KST, 분 단위)")
print("=" * 80)
df1 = q(f"""
SELECT brand,
       CONVERT_TIMEZONE('UTC', 'Asia/Seoul', snapshot_ts) AS kst_ts,
       COUNT(DISTINCT ad_id) AS ads
FROM {TABLE}
WHERE brand IN ('MLB', 'MLB_KIDS')
GROUP BY brand, snapshot_ts
ORDER BY snapshot_ts ASC
""")
print(df1.to_string(index=False))

print("\n" + "=" * 80)
print("[2] 비교 — SERGIO_TACCHINI 최근 24h 스냅샷 시각")
print("=" * 80)
df2 = q(f"""
SELECT CONVERT_TIMEZONE('UTC', 'Asia/Seoul', snapshot_ts) AS kst_ts,
       COUNT(DISTINCT ad_id) AS ads
FROM {TABLE}
WHERE brand = 'SERGIO_TACCHINI'
  AND snapshot_ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY snapshot_ts
ORDER BY snapshot_ts DESC
""")
print(df2.to_string(index=False))
