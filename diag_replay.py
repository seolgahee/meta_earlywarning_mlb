"""특정 시각의 evaluate_alerts() 게이트 통과 여부를 ad 단위로 재현.

production app.py 와 동일한 매칭 로직 사용:
- 6h/12h 전 = ABS(DATEDIFF('minute', snapshot_ts, target)) 가장 작은 row 1개
- 윈도우: BETWEEN target-2h AND target+2h (가까운 미래까지 포함)
- past 매칭 실패 시 fillna(current) → delta=0 → gate fail
"""
import os, sys
from dotenv import load_dotenv
load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# 재현할 production 실행 시각 (UTC). KST 23:01 = UTC 14:01
TARGETS_UTC = ["2026-05-26 14:01", "2026-05-26 12:00", "2026-05-26 08:00", "2026-05-25 12:00"]

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

for tgt in TARGETS_UTC:
    print("=" * 100)
    print(f"  Production 시각 재현: UTC {tgt}  (KST +9h)")
    print("=" * 100)

    # production과 동일: 현재 = 가장 최근 row (per ad), past = ±2h 윈도우 내 가까운 row 1개
    q = f"""
WITH ts AS (SELECT '{tgt}'::TIMESTAMP_NTZ AS now_utc),
cur AS (
  SELECT ad_id, brand, ad_name, campaign_name, snapshot_ts,
         COALESCE(spend_total_cum,     spend_cum)     AS sp,
         COALESCE(purchases_total_cum, purchases_cum) AS pu,
         COALESCE(revenue_total_cum,   revenue_cum)   AS rv,
         ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY snapshot_ts DESC) AS rn
  FROM {TABLE}, ts
  WHERE brand LIKE 'MLB%'
    AND snapshot_ts BETWEEN DATEADD('hour', -1, ts.now_utc) AND ts.now_utc
),
p6 AS (
  SELECT ad_id,
         COALESCE(spend_total_cum,     spend_cum)     AS sp,
         COALESCE(purchases_total_cum, purchases_cum) AS pu,
         COALESCE(revenue_total_cum,   revenue_cum)   AS rv,
         ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY
           ABS(DATEDIFF('minute', snapshot_ts, DATEADD('hour', -6, ts.now_utc)))) AS rn
  FROM {TABLE}, ts
  WHERE brand LIKE 'MLB%'
    AND snapshot_ts BETWEEN DATEADD('hour', -8, ts.now_utc) AND DATEADD('hour', -4, ts.now_utc)
),
p12 AS (
  SELECT ad_id,
         COALESCE(spend_total_cum,     spend_cum)     AS sp,
         COALESCE(purchases_total_cum, purchases_cum) AS pu,
         COALESCE(revenue_total_cum,   revenue_cum)   AS rv,
         ROW_NUMBER() OVER (PARTITION BY ad_id ORDER BY
           ABS(DATEDIFF('minute', snapshot_ts, DATEADD('hour', -12, ts.now_utc)))) AS rn
  FROM {TABLE}, ts
  WHERE brand LIKE 'MLB%'
    AND snapshot_ts BETWEEN DATEADD('hour', -14, ts.now_utc) AND DATEADD('hour', -10, ts.now_utc)
)
SELECT c.ad_id, c.brand, LEFT(c.ad_name, 50) AS ad_short,
       c.sp - COALESCE(p6.sp, c.sp)  AS spend_6h,
       c.pu - COALESCE(p6.pu, c.pu)  AS purch_6h,
       c.rv - COALESCE(p6.rv, c.rv)  AS rev_6h,
       CASE WHEN c.sp - COALESCE(p6.sp, c.sp) > 0
            THEN (c.rv - COALESCE(p6.rv, c.rv)) / (c.sp - COALESCE(p6.sp, c.sp))
            ELSE 0 END AS roas_6h,
       (c.sp - COALESCE(p12.sp, c.sp)) - (c.sp - COALESCE(p6.sp, c.sp))  AS spend_prev_6h,
       CASE WHEN p6.ad_id IS NULL  THEN 'NO_6H'  ELSE 'OK' END AS m6,
       CASE WHEN p12.ad_id IS NULL THEN 'NO_12H' ELSE 'OK' END AS m12
FROM cur c
LEFT JOIN p6  ON c.ad_id = p6.ad_id  AND p6.rn  = 1
LEFT JOIN p12 ON c.ad_id = p12.ad_id AND p12.rn = 1
WHERE c.rn = 1
ORDER BY c.brand, roas_6h DESC
"""
    cur.execute(q)
    rows = cur.fetchall()
    print(f"  현재 시각 스냅샷 ad 수: {len(rows)}")

    abs_pass = trend_pass = full_pass = 0
    print(f"  {'brand':9} {'ad':52} {'spend_6h':>9} {'purch':>6} {'roas_6h':>8} {'sp_prev6h':>10} {'m6':>6} {'m12':>6} {'gate':>5}")
    for r in rows:
        ad_id, brand, ad_short, sp6, pu6, rv6, ro6, spp6, m6, m12 = r
        sp6 = float(sp6 or 0); pu6 = float(pu6 or 0); ro6 = float(ro6 or 0); spp6 = float(spp6 or 0)
        abs_ok   = (ro6 >= 5.0) and (sp6 >= 10000) and (pu6 >= 3)
        # roas_prev_6h = rev_prev_6h / spend_prev_6h. rev_prev_6h 계산 안 했으니 단순화: roas_6h > roas_prev_6h 검사는 별도 필요
        trend_ok_simple = (spp6 >= 10000)   # roas 부분은 별도 진단
        if abs_ok: abs_pass += 1
        if trend_ok_simple: trend_pass += 1
        gate = 'PASS' if abs_ok and trend_ok_simple else ('-' if not abs_ok else 'TRD?')
        if abs_ok or sp6 >= 5000:   # 표시할 가치 있는 행만
            print(f"  {brand:9} {ad_short:52} {sp6:9,.0f} {int(pu6):>6} {ro6:>7.1%} {spp6:>10,.0f} {m6:>6} {m12:>6} {gate:>5}")
    print(f"\n  요약: abs_gate 통과 {abs_pass}건 / spend_prev_6h>=10k {trend_pass}건 / 전체 {len(rows)}건")
    print()

cur.close()
conn.close()
