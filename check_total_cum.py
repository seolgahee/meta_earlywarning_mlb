"""META_AD_SNAPSHOT 의 *_TOTAL_CUM 컬럼 존재/적재 상태 점검."""
import os, sys
from dotenv import load_dotenv
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key, Encoding, PrivateFormat, NoEncryption,
)

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")

SF = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"), "user": os.getenv("SNOWFLAKE_USER"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"), "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"), "role": os.getenv("SNOWFLAKE_ROLE", "PU_PF"),
}
TABLE = f"{SF['database']}.{SF['schema']}.{os.getenv('SNOWFLAKE_TABLE', 'META_AD_SNAPSHOT')}"


def _pk():
    with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), "rb") as f:
        key = load_pem_private_key(f.read(), password=None, backend=default_backend())
    return key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())


conn = snowflake.connector.connect(private_key=_pk(), **SF)
cur = conn.cursor()
cur.execute(f"DESC TABLE {TABLE}")
cols = [r[0].upper() for r in cur.fetchall()]
print(f"[{TABLE}] 컬럼 {len(cols)}개")
total_cols = ["IMPRESSIONS_TOTAL_CUM", "CLICKS_TOTAL_CUM", "SPEND_TOTAL_CUM",
              "PURCHASES_TOTAL_CUM", "REVENUE_TOTAL_CUM"]
for c in total_cols:
    print(f"  {c:<26} {'존재' if c in cols else '*** 없음 ***'}")

if all(c in cols for c in total_cols):
    cur.execute(f"""SELECT COUNT(*) total,
                           COUNT(SPEND_TOTAL_CUM) filled,
                           MIN(CASE WHEN SPEND_TOTAL_CUM IS NOT NULL THEN SNAPSHOT_TS END) first_filled,
                           MAX(SNAPSHOT_TS) latest
                    FROM {TABLE} WHERE BRAND IN ('MLB','MLB_KIDS')""")
    r = cur.fetchone()
    print(f"\n  MLB rows={r[0]}, TOTAL_CUM 채워진 row={r[1]}")
    print(f"  TOTAL_CUM 최초 적재 시각={r[2]}, 최신 스냅샷={r[3]}")

    print("\n  [KST 날짜별 적재 상태]")
    cur.execute(f"""SELECT TO_DATE(CONVERT_TIMEZONE('UTC','Asia/Seoul', SNAPSHOT_TS)) d,
                           COUNT(*) n, COUNT(SPEND_TOTAL_CUM) filled
                    FROM {TABLE} WHERE BRAND IN ('MLB','MLB_KIDS')
                    GROUP BY 1 ORDER BY 1""")
    for d, n, f in cur.fetchall():
        print(f"   {d} | rows {n:>5} | TOTAL_CUM {f:>5} | {'전부' if f==n else ('없음' if f==0 else '일부')}")

    # 한 광고의 TOTAL_CUM vs CUM 추이 샘플 — 누적이 맞게 쌓이는지
    print("\n  [샘플 광고 1개 — TOTAL_CUM 추이]")
    cur.execute(f"""SELECT ad_id FROM {TABLE} WHERE BRAND='MLB' AND SPEND_TOTAL_CUM IS NOT NULL
                    GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 1""")
    sample_ad = cur.fetchone()[0]
    cur.execute(f"""SELECT CONVERT_TIMEZONE('UTC','Asia/Seoul',SNAPSHOT_TS), SPEND_CUM, SPEND_TOTAL_CUM
                    FROM {TABLE} WHERE AD_ID='{sample_ad}' ORDER BY SNAPSHOT_TS""")
    for ts, sc, stc in cur.fetchall():
        print(f"   {ts:%m-%d %H:%M} | SPEND_CUM={sc or 0:>11,.0f} | SPEND_TOTAL_CUM={'NULL' if stc is None else f'{stc:>13,.0f}'}")
conn.close()
