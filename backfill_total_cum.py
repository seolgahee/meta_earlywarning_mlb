"""
META_AD_SNAPSHOT 의 *_TOTAL_CUM 백필 — 자정 무관 통합 누적값 재계산.

app.py load_to_snowflake 의 forward 로직과 동일하게:
  같은 KST 날짜면  delta = max(0, cum_now - prev_cum)   (1시간 증분)
  날짜 바뀌면      delta = max(0, cum_now)               (자정 리셋 → cum 자체가 증분)
  TOTAL_CUM = prev_TOTAL_CUM + delta

기존 부분 백필(05-11~05-15)과 동일 로직이라 재계산해도 값이 같아야 함(검증 포함).

usage:
    python backfill_total_cum.py            # dry-run (계산·검증만, 쓰기 없음)
    python backfill_total_cum.py --apply    # 실제 UPDATE
"""
import os, sys
from datetime import timedelta
from zoneinfo import ZoneInfo

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
    "account": os.getenv("SNOWFLAKE_ACCOUNT"), "user": os.getenv("SNOWFLAKE_USER"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"), "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"), "role": os.getenv("SNOWFLAKE_ROLE", "PU_PF"),
}
TABLE = f"{SF['database']}.{SF['schema']}.{os.getenv('SNOWFLAKE_TABLE', 'META_AD_SNAPSHOT')}"
KST = ZoneInfo("Asia/Seoul")
APPLY = "--apply" in sys.argv

CUM_PAIRS = [
    ("IMPRESSIONS_CUM", "IMPRESSIONS_TOTAL_CUM"),
    ("CLICKS_CUM",      "CLICKS_TOTAL_CUM"),
    ("SPEND_CUM",       "SPEND_TOTAL_CUM"),
    ("PURCHASES_CUM",   "PURCHASES_TOTAL_CUM"),
    ("REVENUE_CUM",     "REVENUE_TOTAL_CUM"),
]
TOTAL_COLS = [t for _, t in CUM_PAIRS]


def _pk():
    with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), "rb") as f:
        key = load_pem_private_key(f.read(), password=None, backend=default_backend())
    return key.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())


def compute(df):
    """ad_id 별 시간순 running total. df 에 *_TOTAL_CUM 컬럼을 채워 반환."""
    df = df.sort_values(["AD_ID", "SNAPSHOT_TS"]).reset_index(drop=True)
    for t in TOTAL_COLS:
        df[t] = 0.0
    for ad_id, g in df.groupby("AD_ID", sort=False):
        prev = None  # dict: ts, kst_date, {cum}, {total}
        for idx in g.index:
            ts = df.at[idx, "SNAPSHOT_TS"]
            kst_date = ts.tz_convert(KST).date()
            # app.py 와 동일: 직전 스냅샷이 2일 초과 갭이면 prev 없음 취급(누적 재시작)
            usable = prev is not None and (ts - prev["ts"]) <= timedelta(days=2)
            cur_total = {}
            for col_cum, col_total in CUM_PAIRS:
                cum_now = float(df.at[idx, col_cum] or 0)
                if usable and prev["kst_date"] == kst_date:
                    delta = max(0.0, cum_now - prev["cum"][col_cum])
                else:
                    delta = max(0.0, cum_now)
                base = prev["total"][col_total] if usable else 0.0
                val = base + delta
                cur_total[col_total] = val
                df.at[idx, col_total] = val
            prev = {
                "ts": ts, "kst_date": kst_date,
                "cum":   {c: float(df.at[idx, c] or 0) for c, _ in CUM_PAIRS},
                "total": cur_total,
            }
    for t in ["IMPRESSIONS_TOTAL_CUM", "CLICKS_TOTAL_CUM", "PURCHASES_TOTAL_CUM"]:
        df[t] = df[t].round().astype("int64")
    return df


def main():
    print(f"[백필] {TABLE}  mode={'APPLY' if APPLY else 'DRY-RUN'}")
    conn = snowflake.connector.connect(private_key=_pk(), **SF)
    cur = conn.cursor()

    cur.execute(f"""
        SELECT AD_ID, SNAPSHOT_TS, BRAND,
               IMPRESSIONS_CUM, CLICKS_CUM, SPEND_CUM, PURCHASES_CUM, REVENUE_CUM,
               IMPRESSIONS_TOTAL_CUM, CLICKS_TOTAL_CUM, SPEND_TOTAL_CUM,
               PURCHASES_TOTAL_CUM, REVENUE_TOTAL_CUM
        FROM {TABLE} WHERE BRAND IN ('MLB','MLB_KIDS')
        ORDER BY AD_ID, SNAPSHOT_TS
    """)
    cols = [d[0] for d in cur.description]
    raw = pd.DataFrame(cur.fetchall(), columns=cols)
    raw["SNAPSHOT_TS"] = pd.to_datetime(raw["SNAPSHOT_TS"], utc=True)
    print(f"  대상 rows={len(raw)}, ads={raw['AD_ID'].nunique()}")

    # 채널 중복 (동일 ad_id+snapshot_ts) dedup — 누적값 동일하므로 first
    uniq = raw.drop_duplicates(subset=["AD_ID", "SNAPSHOT_TS"], keep="first").copy()
    existing = uniq[["AD_ID", "SNAPSHOT_TS"] + TOTAL_COLS].rename(
        columns={t: t + "_OLD" for t in TOTAL_COLS})

    out = compute(uniq[["AD_ID", "SNAPSHOT_TS"] + [c for c, _ in CUM_PAIRS]].copy())

    # ── 검증 1: 기존 백필값과 일치하는지 ──
    chk = out.merge(existing, on=["AD_ID", "SNAPSHOT_TS"], how="left")
    mism = 0
    for t in TOTAL_COLS:
        old = pd.to_numeric(chk[t + "_OLD"], errors="coerce")
        diff = old.notna() & ((old - chk[t]).abs() > 1.0)
        if diff.any():
            mism += int(diff.sum())
            print(f"  [!] {t}: 기존값 불일치 {int(diff.sum())}건")
    filled_old = pd.to_numeric(chk["SPEND_TOTAL_CUM_OLD"], errors="coerce").notna().sum()
    print(f"  검증1) 기존 TOTAL_CUM 보유 row={filled_old} 중 재계산 불일치={mism}건"
          + ("  → 로직 일치 OK" if mism == 0 else "  → 확인 필요!"))

    # ── 검증 2: ad 별 단조 증가 + TOTAL >= CUM ──
    # SPEND_TOTAL_CUM 감소는 2일+ 갭 리셋이면 정상(app.py 동작과 동일), 갭<=2일이면 진짜 버그
    bad_mono = bad_ge = ok_reset = 0
    for ad_id, g in out.groupby("AD_ID"):
        g = g.sort_values("SNAPSHOT_TS").reset_index(drop=True)
        dec = g["SPEND_TOTAL_CUM"].diff() < -1.0
        for i in g.index[dec]:
            gap = g.at[i, "SNAPSHOT_TS"] - g.at[i - 1, "SNAPSHOT_TS"]
            if gap <= timedelta(days=2):
                bad_mono += 1
                print(f"  [!] 단조감소(갭 {gap}): ad={ad_id} "
                      f"{g.at[i-1,'SNAPSHOT_TS']:%m-%d %H:%M}→{g.at[i,'SNAPSHOT_TS']:%m-%d %H:%M}")
            else:
                ok_reset += 1
        for c, t in CUM_PAIRS:
            if (g[t] + 1.0 < pd.to_numeric(g[c], errors="coerce").fillna(0)).any():
                bad_ge += 1
    print(f"  검증2) 진짜 단조감소 위반={bad_mono}, 2일+갭 리셋(정상)={ok_reset}, TOTAL<CUM 위반={bad_ge}"
          + ("  → OK" if bad_mono == 0 and bad_ge == 0 else "  → 확인 필요!"))

    # 샘플 출력
    sample = out[out["AD_ID"] == out.groupby("AD_ID").size().idxmax()]
    print(f"\n  [샘플 ad={sample['AD_ID'].iloc[0]} — 계산 결과 일부]")
    for _, r in sample.head(6).iterrows():
        print(f"   {r['SNAPSHOT_TS'].tz_convert(KST):%m-%d %H:%M} | "
              f"SPEND_CUM={r['SPEND_CUM'] or 0:>11,.0f} | SPEND_TOTAL_CUM={r['SPEND_TOTAL_CUM']:>13,.0f}")

    if mism or bad_mono or bad_ge:
        print("\n  검증 실패 — UPDATE 중단. 로직 점검 필요.")
        conn.close(); return

    if not APPLY:
        print(f"\n  [DRY-RUN] {len(out)}개 (ad,snapshot) 조합 계산 완료. 쓰려면 --apply")
        conn.close(); return

    # ── UPDATE: 임시테이블 적재 후 join 일괄 갱신 ──
    print(f"\n  [APPLY] 임시테이블 적재 중...")
    cur.execute(f"CREATE TEMPORARY TABLE TMP_TOTAL_CUM LIKE {TABLE}")
    ins_cols = ["AD_ID", "SNAPSHOT_TS"] + TOTAL_COLS
    sql_ins = (f"INSERT INTO TMP_TOTAL_CUM ({', '.join(ins_cols)}) "
               f"VALUES ({', '.join(['%s'] * len(ins_cols))})")
    # pandas/numpy 타입 → native python (snowflake 바인딩용). SNAPSHOT_TS 는 NTZ(UTC) naive.
    recs = [(
        str(r["AD_ID"]),
        r["SNAPSHOT_TS"].to_pydatetime().replace(tzinfo=None),
        int(r["IMPRESSIONS_TOTAL_CUM"]), int(r["CLICKS_TOTAL_CUM"]),
        float(r["SPEND_TOTAL_CUM"]), int(r["PURCHASES_TOTAL_CUM"]), float(r["REVENUE_TOTAL_CUM"]),
    ) for _, r in out.iterrows()]
    cur.executemany(sql_ins, recs)
    print(f"  임시테이블 {len(recs)}행 적재 완료. UPDATE 실행...")

    set_clause = ", ".join(f"{t} = s.{t}" for t in TOTAL_COLS)
    cur.execute(f"""
        UPDATE {TABLE} t
        SET {set_clause}
        FROM TMP_TOTAL_CUM s
        WHERE t.AD_ID = s.AD_ID AND t.SNAPSHOT_TS = s.SNAPSHOT_TS
          AND t.BRAND IN ('MLB','MLB_KIDS')
    """)
    print(f"  UPDATE 완료: {cur.rowcount}행 갱신")

    cur.execute(f"""SELECT COUNT(*), COUNT(SPEND_TOTAL_CUM)
                    FROM {TABLE} WHERE BRAND IN ('MLB','MLB_KIDS')""")
    n, f = cur.fetchone()
    print(f"  검증) MLB rows={n}, TOTAL_CUM 채워진 row={f}"
          + ("  → 전부 채워짐 OK" if n == f else "  → 누락 존재!"))
    conn.close()


if __name__ == "__main__":
    main()
