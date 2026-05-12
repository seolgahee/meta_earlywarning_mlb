"""신/구 재고 테이블 스키마/샘플 비교."""
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


for tbl in ["DW_SCS_DACUM", "DB_SH_SCS_STOCK"]:
    print(f"\n{'='*80}\n[{tbl}] 컬럼\n{'='*80}")
    try:
        cols = q(f"""
            SELECT column_name, data_type
            FROM FNF.INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = 'PRCS' AND table_name = '{tbl}'
            ORDER BY ordinal_position
        """)
        print(cols.to_string(index=False))
    except Exception as e:
        print(f"ERROR: {e}")

    print(f"\n[{tbl}] 샘플 5행 (BRD_CD='M' 또는 전체)")
    try:
        sample = q(f"SELECT * FROM FNF.PRCS.{tbl} WHERE BRD_CD IN ('M','I') LIMIT 5")
        print(sample.to_string(index=False))
    except Exception as e:
        try:
            sample = q(f"SELECT * FROM FNF.PRCS.{tbl} LIMIT 5")
            print(sample.to_string(index=False))
        except Exception as e2:
            print(f"ERROR: {e2}")

    # 날짜 컬럼이 있다면 분포 확인
    print(f"\n[{tbl}] 날짜 컬럼 후보 분포")
    for date_col in ["START_DT", "BASE_DT", "STOCK_DT", "DT", "STD_DT", "REG_DT", "BASE_DTM"]:
        try:
            row = q(f"SELECT MIN({date_col}) AS min_dt, MAX({date_col}) AS max_dt, COUNT(DISTINCT {date_col}) AS distinct_dts FROM FNF.PRCS.{tbl}")
            print(f"  {date_col}: {row.to_dict('records')[0]}")
        except Exception:
            pass
