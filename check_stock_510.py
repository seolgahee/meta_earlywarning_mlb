"""자사몰 SHOP_ID=510의 데이터 패턴 — 음수가 어떻게 발생하는지."""
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
pd.set_option("display.width", 280)
pd.set_option("display.max_rows", 50)

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


print("[A] 자사몰 510 (BRD='M') 행 분포 — AVAILABLE_STOCK_QTY 부호별 카운트")
print(q("""
SELECT
  CASE WHEN AVAILABLE_STOCK_QTY > 0 THEN '양수'
       WHEN AVAILABLE_STOCK_QTY = 0 THEN '0'
       WHEN AVAILABLE_STOCK_QTY < 0 THEN '음수' END AS sign_avail,
  COUNT(*) AS cnt,
  SUM(AVAILABLE_STOCK_QTY) AS sum_avail
FROM FNF.PRCS.DB_SH_SCS_STOCK
WHERE SHOP_ID = '510' AND BRD_CD = 'M' AND DT = (SELECT MAX(DT) FROM FNF.PRCS.DB_SH_SCS_STOCK)
GROUP BY sign_avail
""").to_string(index=False))

print("\n[B] 자사몰 510 첫 행 5개 (모든 컬럼 — 음수가 무엇과 연관되는지)")
print(q("""
SELECT *
FROM FNF.PRCS.DB_SH_SCS_STOCK
WHERE SHOP_ID = '510' AND BRD_CD = 'M' AND DT = (SELECT MAX(DT) FROM FNF.PRCS.DB_SH_SCS_STOCK)
  AND AVAILABLE_STOCK_QTY < 0
ORDER BY AVAILABLE_STOCK_QTY ASC
LIMIT 5
""").to_string(index=False))

print("\n[C] 가설검증: AVAILABLE_STOCK_QTY + SALE_QTY 합이 작은가? (판매로 빠진 만큼 음수?)")
print(q("""
SELECT
  SUM(AVAILABLE_STOCK_QTY)             AS sum_avail,
  SUM(SALE_QTY)                        AS sum_sale,
  SUM(DELV_QTY)                        AS sum_delv,
  SUM(AVAILABLE_STOCK_QTY + SALE_QTY)  AS avail_plus_sale,
  SUM(AVAILABLE_STOCK_QTY + DELV_QTY)  AS avail_plus_delv,
  SUM(AVAILABLE_STOCK_QTY + DELV_QTY - SALE_QTY) AS avail_plus_net
FROM FNF.PRCS.DB_SH_SCS_STOCK
WHERE SHOP_ID = '510' AND BRD_CD = 'M' AND DT = (SELECT MAX(DT) FROM FNF.PRCS.DB_SH_SCS_STOCK)
""").to_string(index=False))

print("\n[D] AVAILABLE_STK_STOCK_QTY (매장 SKU 가용재고) 자사몰 510 분포")
print(q("""
SELECT
  CASE WHEN AVAILABLE_STK_STOCK_QTY > 0 THEN '양수'
       WHEN AVAILABLE_STK_STOCK_QTY = 0 THEN '0'
       WHEN AVAILABLE_STK_STOCK_QTY < 0 THEN '음수' END AS sign_stk,
  COUNT(*) AS cnt, SUM(AVAILABLE_STK_STOCK_QTY) AS sum_stk
FROM FNF.PRCS.DB_SH_SCS_STOCK
WHERE SHOP_ID = '510' AND BRD_CD = 'M' AND DT = (SELECT MAX(DT) FROM FNF.PRCS.DB_SH_SCS_STOCK)
GROUP BY sign_stk
""").to_string(index=False))

print("\n[E] DB_SHOP에서 SHOP_ID 510/50002 정의 확인")
print(q("""
SELECT BRD_CD, SHOP_ID, SHOP_NM, ANLYS_DIST_TYPE_CD, OPER_GBN_CD, SALE_AREA_NM
FROM FNF.PRCS.DB_SHOP
WHERE SHOP_ID IN ('510', '50002')
ORDER BY BRD_CD, SHOP_ID
""").to_string(index=False))
