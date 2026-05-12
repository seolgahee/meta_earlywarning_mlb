"""자사몰 SHOP_ID 데이터 존재 및 컬럼별 값 확인."""
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
pd.set_option("display.max_rows", 100)

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


def q(sql):
    conn = snowflake.connector.connect(private_key=_pk(), **SF)
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


print("[1] 자사몰 SHOP_ID(510, 50002) 데이터 유무")
print(q("""
SELECT BRD_CD, SHOP_ID, COUNT(*) AS row_cnt,
       SUM(AVAILABLE_STOCK_QTY) AS sum_avail,
       SUM(ACTUAL_AVAILABLE_STOCK_QTY) AS sum_actual,
       SUM(AVAILABLE_STK_STOCK_QTY) AS sum_stk,
       SUM(DELV_QTY) AS sum_delv,
       SUM(SALE_QTY) AS sum_sale
FROM FNF.PRCS.DB_SH_SCS_STOCK
WHERE SHOP_ID IN ('510', '50002')
GROUP BY BRD_CD, SHOP_ID
ORDER BY BRD_CD, SHOP_ID
""").to_string(index=False))

print("\n[2] BRD_CD='M' 전체 SHOP_ID별 가용재고 TOP 15 (어떤 매장이 큰지)")
print(q("""
SELECT SHOP_ID,
       COUNT(*) AS row_cnt,
       SUM(AVAILABLE_STOCK_QTY) AS sum_avail,
       SUM(ACTUAL_AVAILABLE_STOCK_QTY) AS sum_actual,
       SUM(AVAILABLE_STK_STOCK_QTY) AS sum_stk
FROM FNF.PRCS.DB_SH_SCS_STOCK
WHERE BRD_CD = 'M' AND DT = (SELECT MAX(DT) FROM FNF.PRCS.DB_SH_SCS_STOCK)
GROUP BY SHOP_ID
ORDER BY sum_avail DESC NULLS LAST
LIMIT 15
""").to_string(index=False))

print("\n[3] BRD_CD='I' 동일")
print(q("""
SELECT SHOP_ID,
       COUNT(*) AS row_cnt,
       SUM(AVAILABLE_STOCK_QTY) AS sum_avail,
       SUM(ACTUAL_AVAILABLE_STOCK_QTY) AS sum_actual,
       SUM(AVAILABLE_STK_STOCK_QTY) AS sum_stk
FROM FNF.PRCS.DB_SH_SCS_STOCK
WHERE BRD_CD = 'I' AND DT = (SELECT MAX(DT) FROM FNF.PRCS.DB_SH_SCS_STOCK)
GROUP BY SHOP_ID
ORDER BY sum_avail DESC NULLS LAST
LIMIT 15
""").to_string(index=False))

print("\n[4] 특정 품번 1개로 신/구 테이블 결과 비교 (BRD='M' 첫 품번)")
sample = q("""
SELECT PART_CD FROM FNF.PRCS.DB_SH_SCS_STOCK
WHERE BRD_CD = 'M' AND AVAILABLE_STOCK_QTY > 0
LIMIT 1
""")
if not sample.empty:
    part = sample.iloc[0]["PART_CD"]
    print(f"  비교 PART_CD = {part}")

    print(f"\n  -- 신 테이블: SHOP_ID별 가용재고 (PART_CD='{part}')")
    print(q(f"""
SELECT SHOP_ID, COLOR_CD, SIZE_CD,
       AVAILABLE_STOCK_QTY, ACTUAL_AVAILABLE_STOCK_QTY, AVAILABLE_STK_STOCK_QTY
FROM FNF.PRCS.DB_SH_SCS_STOCK
WHERE BRD_CD='M' AND PART_CD='{part}' AND DT = (SELECT MAX(DT) FROM FNF.PRCS.DB_SH_SCS_STOCK)
  AND AVAILABLE_STOCK_QTY > 0
ORDER BY SHOP_ID
LIMIT 15
""").to_string(index=False))

    print(f"\n  -- 구 테이블 DW_SCS_DACUM: 같은 PART_CD WH_STOCK_QTY/STOCK_QTY")
    print(q(f"""
SELECT COLOR_CD, SIZE_CD,
       SUM(WH_STOCK_QTY) AS wh_stock, SUM(STOCK_QTY) AS total_stock
FROM FNF.PRCS.DW_SCS_DACUM
WHERE BRD_CD='M' AND PART_CD='{part}'
  AND START_DT = (SELECT MAX(START_DT) FROM FNF.PRCS.DW_SCS_DACUM
                  WHERE BRD_CD='M' AND PART_CD='{part}' AND START_DT <= CURRENT_DATE)
GROUP BY COLOR_CD, SIZE_CD
ORDER BY COLOR_CD, SIZE_CD
""").to_string(index=False))
