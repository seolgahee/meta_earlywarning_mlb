"""BR 캠페인의 ad_name에서 품번 추출 가능 여부 확인."""
import os, sys, re
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
TABLE = f"{SF['database']}.{SF['schema']}.{os.getenv('SNOWFLAKE_TABLE','META_AD_SNAPSHOT')}"

def _pk():
    with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), "rb") as f:
        return load_pem_private_key(f.read(), password=None, backend=default_backend()).private_bytes(
            Encoding.DER, PrivateFormat.PKCS8, NoEncryption())

def qs(sql):
    c = snowflake.connector.connect(private_key=_pk(), **SF)
    try: return pd.read_sql(sql, c)
    finally: c.close()

# app.py와 동일 패턴 (\d{2,4}) + 컬러 코드 검출용
PART_RE = re.compile(r'(?:^|[-_])([37][A-Z]{3,5}\d{2,4}[A-Z]?)(?=[-_]|$)')
# 품번-컬러 형식: 품번 뒤에 - 그리고 컬러
PART_COLOR_RE = re.compile(r'(?:^|[-_])([37][A-Z]{3,5}\d{2,4}[A-Z]?)-([0-9A-Z]+)(?=[-_]|$)')

def extract(name: str):
    return PART_RE.findall(str(name)) if name else []

def extract_with_color(name: str):
    return PART_COLOR_RE.findall(str(name)) if name else []

def has_br_token(name: str) -> bool:
    return "BR" in [t.upper() for t in re.split(r"[\s_\-|/]+", str(name))] if name else False

df = qs(f"""
SELECT DISTINCT brand, campaign_name, ad_id, ad_name
FROM {TABLE}
WHERE brand IN ('MLB','MLB_KIDS')
ORDER BY brand, campaign_name, ad_name
""")
df.columns = [c.upper() for c in df.columns]
df["PART_CODES"] = df["AD_NAME"].apply(extract)
df["PART_COLOR"] = df["AD_NAME"].apply(extract_with_color)
df["HAS_CODE"] = df["PART_CODES"].apply(lambda x: bool(x))
df["HAS_COLOR"] = df["PART_COLOR"].apply(lambda x: bool(x))

print(f"전체 광고 {len(df)}개 (성인+키즈, BR+Performance)")
print(f"  품번 파싱 OK: {df['HAS_CODE'].sum()}")
print(f"  품번-컬러 OK: {df['HAS_COLOR'].sum()}")
print()

# 컬러까지 잡힌 케이스만 분류 추출
print("=" * 100)
print("[1] 품번-컬러 형식으로 잡힌 광고 (컬러 코드 분포 확인)")
print("=" * 100)
color_rows = df[df["HAS_COLOR"]]
for _, r in color_rows.iterrows():
    for pc, cc in r["PART_COLOR"]:
        print(f"  {r['BRAND']:9s} | part={pc:14s} color={cc:8s} | {r['AD_NAME']}")

# 컬러 코드 형식 통계
print("\n" + "=" * 100)
print("[2] 등장한 컬러 코드 unique 목록 (형식 파악)")
print("=" * 100)
all_colors = []
for codes in df["PART_COLOR"]:
    all_colors.extend([cc for _, cc in codes])
import collections
ctr = collections.Counter(all_colors)
for cc, n in ctr.most_common():
    print(f"  '{cc}' (len={len(cc)}): {n}회")
