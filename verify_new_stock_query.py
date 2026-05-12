"""신 쿼리(DB_SH_SCS_STOCK)가 정상 동작하는지 PART_CD 1~2개로 검증."""
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

SF = dict(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE", "PU_PF"),
)


def _pk():
    with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), "rb") as f:
        key = load_pem_private_key(f.read(), password=None, backend=default_backend())
    return key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())


def q(sql, params=()):
    conn = snowflake.connector.connect(private_key=_pk(), **SF)
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()


# PART_CD가 9001 EC물류에 양수 가용재고를 가진 거 1개 찾기
print("[STEP 1] EC물류 9001 (성인) 양수 PART_CD 1개 찾기")
df = q("""
SELECT PART_CD, SUM(AVAILABLE_STOCK_QTY) AS avail
FROM FNF.PRCS.DB_SH_SCS_STOCK
WHERE BRD_CD='M' AND SHOP_ID='9001'
  AND DT = (SELECT MAX(DT) FROM FNF.PRCS.DB_SH_SCS_STOCK WHERE DT <= CURRENT_DATE)
  AND AVAILABLE_STOCK_QTY > 0
GROUP BY PART_CD
ORDER BY avail DESC
LIMIT 3
""")
print(df.to_string(index=False))

if df.empty:
    print("EC물류 양수 재고 없음 → 검증 불가")
    raise SystemExit(0)

target_part = df.iloc[0]["PART_CD"]
print(f"\n[STEP 2] PART_CD={target_part} — 컬러별 새 쿼리 결과 (color_cd=None 경로)")

color_sql = """
SELECT d.COLOR_CD,
       SUM(CASE WHEN d.SHOP_ID = %s THEN d.AVAILABLE_STOCK_QTY ELSE 0 END) AS WH_STOCK,
       SUM(d.AVAILABLE_STOCK_QTY) AS TOTAL_STOCK,
       MAX(p.PRDT_NM)             AS PRDT_NM
FROM FNF.PRCS.DB_SH_SCS_STOCK d
JOIN FNF.PRCS.DB_SHOP sh
  ON sh.BRD_CD = d.BRD_CD AND sh.SHOP_ID = d.SHOP_ID
LEFT JOIN FNF.PRCS.DB_PRDT p
  ON d.PRDT_CD = p.PRDT_CD
WHERE d.BRD_CD = %s AND d.PART_CD = %s
  AND d.DT = (SELECT MAX(DT) FROM FNF.PRCS.DB_SH_SCS_STOCK WHERE DT <= CURRENT_DATE)
  AND (d.SHOP_ID = %s OR sh.ANLYS_DIST_TYPE_CD = 'F4')
GROUP BY d.COLOR_CD
ORDER BY WH_STOCK DESC
"""
ec_shop = "9001"
print(q(color_sql, (ec_shop, "M", target_part, ec_shop)).to_string(index=False))

print(f"\n[STEP 3] 사이즈별 (위 결과 첫 COLOR_CD)")
df2 = q(color_sql, (ec_shop, "M", target_part, ec_shop))
if not df2.empty:
    first_color = df2.iloc[0]["COLOR_CD"]
    print(f"  COLOR_CD={first_color}")
    size_sql = """
SELECT d.SIZE_CD,
       SUM(CASE WHEN d.SHOP_ID = %s THEN d.AVAILABLE_STOCK_QTY ELSE 0 END) AS WH_STOCK,
       SUM(d.AVAILABLE_STOCK_QTY) AS TOTAL_STOCK,
       MAX(p.PRDT_NM)             AS PRDT_NM
FROM FNF.PRCS.DB_SH_SCS_STOCK d
JOIN FNF.PRCS.DB_SHOP sh
  ON sh.BRD_CD = d.BRD_CD AND sh.SHOP_ID = d.SHOP_ID
LEFT JOIN FNF.PRCS.DB_PRDT p
  ON d.PRDT_CD = p.PRDT_CD
WHERE d.BRD_CD = %s AND d.PART_CD = %s AND d.COLOR_CD = %s
  AND d.DT = (SELECT MAX(DT) FROM FNF.PRCS.DB_SH_SCS_STOCK WHERE DT <= CURRENT_DATE)
  AND (d.SHOP_ID = %s OR sh.ANLYS_DIST_TYPE_CD = 'F4')
GROUP BY d.SIZE_CD
"""
    print(q(size_sql, (ec_shop, "M", target_part, first_color, ec_shop)).to_string(index=False))

print("\n[STEP 4] 키즈(I) 동일 검증")
df3 = q("""
SELECT PART_CD, SUM(AVAILABLE_STOCK_QTY) AS avail
FROM FNF.PRCS.DB_SH_SCS_STOCK
WHERE BRD_CD='I' AND SHOP_ID='90007'
  AND DT = (SELECT MAX(DT) FROM FNF.PRCS.DB_SH_SCS_STOCK WHERE DT <= CURRENT_DATE)
  AND AVAILABLE_STOCK_QTY > 0
GROUP BY PART_CD
ORDER BY avail DESC
LIMIT 2
""")
print(df3.to_string(index=False))
if not df3.empty:
    kid_part = df3.iloc[0]["PART_CD"]
    print(f"\n  키즈 PART_CD={kid_part} 컬러별:")
    print(q(color_sql, ("90007", "I", kid_part, "90007")).to_string(index=False))
