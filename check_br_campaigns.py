"""BR 캠페인 존재 여부 + CTR 분포 확인."""
import os, sys
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
        key = load_pem_private_key(f.read(), password=None, backend=default_backend())
    return key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())

def q(sql):
    c = snowflake.connector.connect(private_key=_pk(), **SF)
    try: return pd.read_sql(sql, c)
    finally: c.close()

print("=" * 80)
print("[1] MLB 캠페인 토큰 분포 (BR 포함 여부)")
print("=" * 80)
df = q(f"""
SELECT brand, campaign_name, COUNT(*) AS row_cnt, COUNT(DISTINCT ad_id) AS ads
FROM {TABLE}
WHERE brand IN ('MLB','MLB_KIDS')
GROUP BY brand, campaign_name
ORDER BY brand, campaign_name
""")
print(df.to_string(index=False))

# 토큰 split 후 BR 매치 광고만
print("\n" + "=" * 80)
print("[2] BR 토큰 매치 캠페인")
print("=" * 80)
import re
def has_br_token(name: str) -> bool:
    if not name: return False
    return "BR" in [t.upper() for t in re.split(r"[\s_\-|/]+", str(name))]
df["IS_BR"] = df["CAMPAIGN_NAME"].apply(has_br_token)
br_only = df[df["IS_BR"]]
if br_only.empty:
    print("  BR 토큰 매치 캠페인 없음 — 현재 데이터로 BR 임계치 튜닝 불가")
else:
    print(br_only.to_string(index=False))
