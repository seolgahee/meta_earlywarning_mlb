"""
Meta Ads 조기경보 시스템
- ASC 캠페인 구조 기반 action_type 분기
- Gemini AI 인사이트
- Office365 SMTP 이메일 발송
"""

import os
import re
import json
import unicodedata
import smtplib
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key, Encoding, PrivateFormat, NoEncryption
from google import genai

load_dotenv()

# ─────────────────────────────────────────
# 설정값
# ─────────────────────────────────────────
ACCESS_TOKEN  = os.getenv("META_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.getenv("META_AD_ACCOUNT_ID")
API_VERSION   = os.getenv("META_API_VERSION", "v19.0")

SNOWFLAKE_ACCOUNT         = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER            = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PRIVATE_KEY     = os.getenv("SNOWFLAKE_PRIVATE_KEY")
SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
SNOWFLAKE_WAREHOUSE   = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE", "PU_PF")
SNOWFLAKE_TABLE     = os.getenv("SNOWFLAKE_TABLE", "META_AD_SNAPSHOT")

SMTP_SERVER      = os.getenv("SMTP_SERVER", "smtp.office365.com")
SMTP_PORT        = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER        = os.getenv("SMTP_USER")
SMTP_PASSWORD    = os.getenv("SMTP_PASSWORD")
GEMINI_API_KEY    = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL      = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

ALERT_LOG_FILE = "alert_sent_log.json"

SNOWFLAKE_STOCK_SCHEMA = os.getenv("SNOWFLAKE_STOCK_SCHEMA", "PRCS")

# 한 레포에서 두 브랜드를 동시 처리. fetch_insights는 공유 META 계정에서 1회 호출하고
# 캠페인명 토큰으로 분기해 cfg별로 적재/평가/알럿을 돌린다.
# jasamol_shop_id: DB_SHOP에서 ANLYS_DIST_TYPE_CD='N1' 메인 자사몰 SHOP_ID
BRAND_CONFIGS = [
    {
        "brand":            "MLB",
        "campaign_token":   "M",
        "stock_brand_cd":   "M",
        "jasamol_shop_id":  "510",     # (주)에프앤에프 MLB 성인 자사몰 (판매 집계용, 재고는 매대 0)
        "ec_logistics_shop_id": "9001",  # EC-온라인물류 (실제 가용재고 보유)
        "slack_webhook":    os.getenv("SLACK_WEBHOOK_URL", ""),
        "alert_recipients": os.getenv("ALERT_RECIPIENTS", ""),
    },
    {
        "brand":            "MLB_KIDS",
        "campaign_token":   "I",
        "stock_brand_cd":   "I",
        "jasamol_shop_id":  "50002",   # (주)에프앤에프 MLB 키즈 자사몰 (판매 집계용)
        "ec_logistics_shop_id": "90007",  # EC-온라인물류 (실제 가용재고 보유)
        "slack_webhook":    os.getenv("SLACK_WEBHOOK_URL_KIDS", ""),
        "alert_recipients": os.getenv("ALERT_RECIPIENTS_KIDS", ""),
    },
]


def filter_by_campaign_token(df: "pd.DataFrame", token: str) -> "pd.DataFrame":
    """campaign_name을 underscore로 split 후 첫 번째 필드(prefix)가 token과
    정확히 일치하는 행만 남긴다. M/I는 단일 문자라 substring 매칭은 위험하므로
    구분자 split 후 정확 일치를 사용한다.
    """
    if df.empty:
        return df

    token_upper = token.upper()
    matched_mask = df["CAMPAIGN_NAME"].fillna("").apply(
        lambda name: name.split("_")[0].strip().upper() == token_upper
    )

    matched   = df[matched_mask]
    unmatched = df[~matched_mask]

    print(f"  [filter token={token}] 매칭 {len(matched)}행 / 미매칭 {len(unmatched)}행")
    if len(unmatched) > 0:
        unmatched_samples = unmatched["CAMPAIGN_NAME"].drop_duplicates().head(3).tolist()
        print(f"  [filter token={token}] 미매칭 캠페인명 샘플 3개: {unmatched_samples}")

    return matched


def tag_brand(df: "pd.DataFrame", brand: str) -> "pd.DataFrame":
    """DataFrame의 BRAND 컬럼을 cfg['brand'] 값으로 채움."""
    df = df.copy()
    df["BRAND"] = brand
    return df

if not ACCESS_TOKEN or not AD_ACCOUNT_ID:
    print("[오류] .env에 META_ACCESS_TOKEN, META_AD_ACCOUNT_ID 값이 없습니다.")
    exit(1)

_gemini_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None


# ─────────────────────────────────────────
# 운영 시간 체크 (KST 01:00 ~ 07:00 실행 금지)
# ─────────────────────────────────────────
def check_operating_hours() -> None:
    kst_now = datetime.now(ZoneInfo("Asia/Seoul"))
    hour    = kst_now.hour
    if 1 <= hour < 7:
        print(f"[종료] 새벽 시간대이므로 실행하지 않음 (현재 KST: {kst_now.strftime('%Y-%m-%d %H:%M')})")
        exit(0)
    print(f"[정보] 운영 시간 확인 완료 (현재 KST: {kst_now.strftime('%Y-%m-%d %H:%M')})")


def check_recent_snapshot_skip(window_minutes: int = 30) -> None:
    """직전 window_minutes 내 MLB 적재 있으면 종료.
    GitHub cron + cron-job.org 동시 trigger 시 정각 중복 실행/알럿 방지.
    """
    if not all([SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER,
                (SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PRIVATE_KEY_PATH)]):
        return
    try:
        conn = get_snowflake_conn()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT MAX(SNAPSHOT_TS)
            FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}
            WHERE BRAND IN ('MLB','MLB_KIDS')
              AND SNAPSHOT_TS >= DATEADD(minute, -{window_minutes}, CURRENT_TIMESTAMP)
        """)
        row = cur.fetchone()
        conn.close()
        if row and row[0]:
            print(f"[종료] {window_minutes}분 내 이미 적재됨 (최근 SNAPSHOT_TS={row[0]}). 중복 실행 방지.")
            exit(0)
    except Exception as e:
        print(f"[경고] 최근 적재 체크 실패 (계속 진행): {e}")


# ─────────────────────────────────────────
# Alert 조건 (운영 기준)
# ─────────────────────────────────────────
# Opportunity 공통 필터 (Performance 캠페인 전용 — BR은 BR_ALERT_CONDITIONS_BY_BRAND 사용)
# 브랜드별 분기: 2026-05-13 KST 42h 분석 결과 (광고당 ~6 샘플, rolling 6h window)
#   성인은 평소 전환 볼륨이 높아(6h당 0.65건 baseline) purch>=3로 강화
#   키즈는 spend 분포가 성인 절반이고 ROAS p75=8.16으로 우수 → spend 컷 완화 + ROAS 강화
OPP_FILTERS = {
    "MLB": {        # 성인
        "purchases_6h_min":  3,
        "spend_6h_min":      10_000,
        "roas_6h_min":       4.0,   # 400%
    },
    "MLB_KIDS": {   # 키즈
        "purchases_6h_min":  2,
        "spend_6h_min":      5_000,
        "roas_6h_min":       5.0,   # 500%
    },
}


def _get_opp_filter(brand: str) -> dict:
    return OPP_FILTERS.get(brand, OPP_FILTERS["MLB"])

# action_type 분기 조건 (우선순위: CAMPAIGN_SCALE > PRODUCT_EXTRACTION > CREATIVE_EXPANSION)
# ASC 통합 구조 기준: ad_id 단위 개별 소재 예산 조정 불가 → 캠페인 일cap/일예산 조정만 가능
# 2026-05-13 튜닝: entry gate (OPP_FILTERS)와 일관성 유지 — 각 분기는 entry보다 strict해야 의미 있음
_GUIDE_SCALE = "전환 효율이 급증한 구간입니다. ASC 캠페인 일cap 상향을 검토하세요."
_GUIDE_EXTRACT = "해당 소재 내 상품을 확인하여 동일 상품 기반 신규 소재 2~3종 추가 제작을 권장합니다."
ACTION_CONDITIONS_BY_BRAND = {
    "MLB": {        # 성인: entry roas≥4, spend≥10k, purch≥3
        "CAMPAIGN_SCALE":   {"roas_6h_min": 4.0, "purchases_6h_min": 4,       "guide": _GUIDE_SCALE},
        "PRODUCT_EXTRACTION":{"roas_6h_min": 4.0, "spend_6h_min": 50_000,     "guide": _GUIDE_EXTRACT},
        "CREATIVE_EXPANSION":{"roas_6h_min": 4.0, "purchases_6h_min": 3,       "guide": _GUIDE_EXTRACT},
    },
    "MLB_KIDS": {   # 키즈: entry roas≥5, spend≥5k, purch≥2
        "CAMPAIGN_SCALE":   {"roas_6h_min": 5.0, "purchases_6h_min": 3,       "guide": _GUIDE_SCALE},
        "PRODUCT_EXTRACTION":{"roas_6h_min": 5.0, "spend_6h_min": 25_000,     "guide": _GUIDE_EXTRACT},
        "CREATIVE_EXPANSION":{"roas_6h_min": 5.0, "purchases_6h_min": 2,       "guide": _GUIDE_EXTRACT},
    },
}


def _get_action_cond(brand: str, action_type: str) -> dict:
    return ACTION_CONDITIONS_BY_BRAND.get(brand, ACTION_CONDITIONS_BY_BRAND["MLB"])[action_type]


# BR(브랜딩) 캠페인 전용 알럿 조건 — 전환 지표 사용 안 함
# 2026-05-13 튜닝: 42h 분석 기준 (성인 60샘플, 키즈 28샘플)
#   현행 imp≥10k/clk≥200은 둘 다 0건 통과 → 광고가 발생하는 분포 기준으로 완화
#   ratio는 SURGE/DROP 각각 ±5% margin (CTR p25=0.77, p75=1.04 분포 반영)
BR_ALERT_CONDITIONS_BY_BRAND = {
    "MLB": {        # 성인: IMP p75=1879, CLK p25=17 → entry 완화
        "impressions_6h_min": 2_000,
        "clicks_6h_min":      30,
        "ctr_surge_ratio":    1.05,
        "ctr_drop_ratio":     0.85,
    },
    "MLB_KIDS": {   # 키즈: 표본 부족 (28샘플, valid CTR 11) → 보수적 적용
        "impressions_6h_min": 1_000,
        "clicks_6h_min":      20,
        "ctr_surge_ratio":    1.05,
        "ctr_drop_ratio":     0.85,
    },
}


def _get_br_cond(brand: str) -> dict:
    return BR_ALERT_CONDITIONS_BY_BRAND.get(brand, BR_ALERT_CONDITIONS_BY_BRAND["MLB"])

# Kill Alert 조건
KILL_CONDITION = {
    "roas_12h_max":  1.2,     # 120%   # TODO(per-brand): MLB 성인/키즈 재산출 대상
    "spend_12h_min": 150_000,         # TODO(per-brand): MLB 성인/키즈 재산출 대상
}

# 재고 일수(DoS) 컷오프 — 과거 30일 정확도 시뮬레이션 기반 (Precision 최적)
# 성인: 보충 변동성 큼 → 짧은 컷오프로 진짜 위급만 / 키즈: 더 예측 가능 → 약간 여유
# urgent: 광고 OFF/즉시 보충, warning: 물류 이동 검토
DOS_CUTOFFS = {
    "M": {"urgent": 3,  "warning": 7},    # MLB 성인 (X<3 Precision 32.5%)
    "I": {"urgent": 5,  "warning": 10},   # MLB 키즈 (X<5 Precision 30.5%)
}
DOS_CUTOFF_DEFAULT = {"urgent": 7, "warning": 14}


def _dos_cutoffs(brand_cd: str) -> dict:
    return DOS_CUTOFFS.get(brand_cd, DOS_CUTOFF_DEFAULT)


def _o2o_signal(online_dos, offline_stock: int, online_daily_avg: float, brand_cd: str):
    """온라인 부족 + 오프라인 적체 시 'URGENT'/'SUGGEST' 반환, 아니면 None.
    offline_dos는 '매장재고 / 자사몰 일평균'으로 환산한 일수.
    """
    if online_dos is None or online_daily_avg <= 0 or offline_stock <= 0:
        return None
    cutoffs = _dos_cutoffs(brand_cd)
    offline_dos = offline_stock / online_daily_avg
    if online_dos < cutoffs["urgent"] and offline_dos > O2O_OFFLINE_DOS_URGENT:
        return "URGENT"
    if online_dos < cutoffs["warning"] and offline_dos > O2O_OFFLINE_DOS_SUGGEST:
        return "SUGGEST"
    return None


# O2O 재배치 신호 — 오프라인 잉여재고 환산 일수 기준
# offline_dos = (전체 매장 재고) / 자사몰 일평균
# URGENT: 온라인 < urgent AND 오프라인 환산 > 14일 (= 매장 재고만으로 자사몰 2주 운영 가능)
# SUGGEST: 온라인 < warning AND 오프라인 환산 > 7일
O2O_OFFLINE_DOS_URGENT  = 14
O2O_OFFLINE_DOS_SUGGEST = 7

# 매장 유형 분류 (DB_SHOP.ANLYS_DIST_TYPE_CD → 표시명)
SHOP_TYPE_MAP = {
    "AX": "백화점",   # 백화점(대형)
    "AS": "백화점",   # 백화점(중소)
    "S2": "대리점",   # 가두(대리)
    "S1": "직영점",   # 가두(직영)
}


# ─────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────
def dw(s: str) -> int:
    """터미널/슬랙 코드블록 기준 표시 너비 (CJK 2칸, 나머지 1칸)."""
    return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in str(s))


def rjust_dw(s: str, width: int) -> str:
    """display width 기준 우측 정렬 패딩."""
    return " " * max(0, width - dw(s)) + str(s)


def ljust_dw(s: str, width: int) -> str:
    """display width 기준 좌측 정렬 패딩."""
    return str(s) + " " * max(0, width - dw(s))


PURCHASE_ACTION_TYPES = [
    "omni_purchase",
    "offsite_conversion.fb_pixel_purchase",
    "purchase",
]


def extract_purchase_count(actions: list) -> int:
    if not actions:
        return 0
    action_map = {item.get("action_type"): item for item in actions}
    for atype in PURCHASE_ACTION_TYPES:
        if atype in action_map:
            return int(float(action_map[atype].get("value", 0)))
    return 0


def extract_purchase_revenue(action_values: list) -> float:
    if not action_values:
        return 0.0
    action_map = {item.get("action_type"): item for item in action_values}
    for atype in PURCHASE_ACTION_TYPES:
        if atype in action_map:
            return float(action_map[atype].get("value", 0.0))
    return 0.0


def extract_product_code(ad_name: str) -> tuple[str, str] | tuple[None, None]:
    """
    소재명에서 MLB 품번 목록 추출. 컬러는 DB에 별도 컬럼이라 None 처리.
    MLB 품번 패턴: [3 또는 7] + 영문3~5 + 숫자3~4 + 영문0~1
      성인: 3으로 시작 (예: 3ACPB245N, 3FTSB0263)
      키즈: 7으로 시작 (예: 7ARNCB063)
    """
    codes = re.findall(r'(?:^|[-_])([37][A-Z]{3,5}\d{3,4}[A-Z]?)(?=[-_]|$)', ad_name)
    if codes:
        return [(c, None) for c in codes]
    return []


def resolve_stock_for_ad(ad_name: str, cfg: dict):
    """ad_name 에서 품번 파싱 → 재고 조회.
    반환: (stock_info, stock_summary, stock_md_guide, stock_product)
    """
    product_codes = extract_product_code(ad_name)
    seen: set = set()
    unique = []
    for pc, cc in product_codes:
        if (pc, cc) not in seen:
            seen.add((pc, cc))
            unique.append((pc, cc))

    items = []
    for pc, cc in unique:
        print(f"  [재고조회] brand={cfg['brand']} stock_brand_cd={cfg['stock_brand_cd']} part={pc} color={cc or '-'}")
        info = fetch_stock_info(pc, cc, cfg["stock_brand_cd"],
                                cfg["jasamol_shop_id"], cfg["ec_logistics_shop_id"])
        if info:
            items.append(info)
        else:
            print(f"  [경고] 재고 조회 결과 없음: {pc}-{cc}")

    stock_info = items[0] if len(items) == 1 else (items if items else None)
    stock_summary  = format_stock_summary(stock_info)
    stock_md_guide = format_stock_md_guide(stock_info)
    stock_product  = "_".join(
        f"{pc}" + (f"-{cc}" if cc else "") for pc, cc in unique
    ) if unique else ""
    if stock_info:
        print(f"  -> 재고: {stock_summary}")
    return stock_info, stock_summary, stock_md_guide, stock_product


_SIZE_ORDER = {"XS": 0, "S": 1, "M": 2, "L": 3, "XL": 4, "XXL": 5, "XXXL": 6}


def fetch_stock_info(part_cd: str, color_cd: str, stock_brand_cd: str,
                     jasamol_shop_id: str, ec_logistics_shop_id: str) -> dict | None:
    """
    DB_SH_SCS_STOCK(EC 기준 매장별 재고) + DW_SH_SCS_D(자사몰/오프라인 판매) 기반.
    - 사용 컬럼: AVAILABLE_STK_STOCK_QTY (매장 SKU 가용재고).
      AVAILABLE_STOCK_QTY 는 EC가 직접 핸들링한 누계라 백화점 등에서 음수 → 신뢰 불가.
    - 온라인재고(wh):  SHOP_ID = ec_logistics_shop_id (EC-온라인물류, dist_type=N1)
    - 전체재고(total): EC물류 + 오프라인매장 합산
      오프라인 매장 = ANLYS_DIST_TYPE_CD IN ('AX','AS','S2','S1') (백화점/대리점/직영점)
    반환: {
        "prdt_nm": "W 하이웨이스트 우븐 플리츠 스커트",
        "is_mc": False,
        "sizes": [{"size": "M", "wh": 5, "total": 12}, ...],
        "weekly_qty": 8,
        "weeks_of_supply": 1.2,
    }
    실패 시 None.
    """
    if not all([SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, (SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PRIVATE_KEY_PATH)]):
        return None
    try:
        conn = get_snowflake_conn()
        cursor = conn.cursor()

        # 최신 스냅샷 일자 — DT는 보통 단일이지만 안전하게 CURRENT_DATE 이하 MAX
        latest_dt_sub = f"""
            SELECT MAX(DT)
            FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_STOCK_SCHEMA}.DB_SH_SCS_STOCK
            WHERE DT <= CURRENT_DATE
        """
        # WHERE 절: EC물류 SHOP 또는 오프라인 매장(백화점 AX/AS, 대리점 S2, 직영점 S1)만 포함
        # 자사몰 N1·외부몰(F1/F3)·면세점(D) 등 제외
        shop_filter = "AND (d.SHOP_ID = %s OR sh.ANLYS_DIST_TYPE_CD IN ('AX','AS','S2','S1'))"

        if color_cd:
            # 단일 컬러 → 사이즈별
            cursor.execute(f"""
                SELECT d.SIZE_CD,
                       SUM(CASE WHEN d.SHOP_ID = %s THEN d.AVAILABLE_STK_STOCK_QTY ELSE 0 END) AS WH_STOCK,
                       SUM(d.AVAILABLE_STK_STOCK_QTY) AS TOTAL_STOCK,
                       MAX(p.PRDT_NM)                 AS PRDT_NM
                FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_STOCK_SCHEMA}.DB_SH_SCS_STOCK d
                JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_STOCK_SCHEMA}.DB_SHOP sh
                  ON sh.BRD_CD = d.BRD_CD AND sh.SHOP_ID = d.SHOP_ID
                LEFT JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_STOCK_SCHEMA}.DB_PRDT p
                  ON d.PRDT_CD = p.PRDT_CD
                WHERE d.BRD_CD = %s AND d.PART_CD = %s AND d.COLOR_CD = %s
                  AND d.DT = ({latest_dt_sub})
                  {shop_filter}
                GROUP BY d.SIZE_CD
            """, (ec_logistics_shop_id, stock_brand_cd, part_cd, color_cd, ec_logistics_shop_id))
        else:
            # 전체 컬러 → 컬러별 합산
            cursor.execute(f"""
                SELECT d.COLOR_CD,
                       SUM(CASE WHEN d.SHOP_ID = %s THEN d.AVAILABLE_STK_STOCK_QTY ELSE 0 END) AS WH_STOCK,
                       SUM(d.AVAILABLE_STK_STOCK_QTY) AS TOTAL_STOCK,
                       MAX(p.PRDT_NM)                 AS PRDT_NM
                FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_STOCK_SCHEMA}.DB_SH_SCS_STOCK d
                JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_STOCK_SCHEMA}.DB_SHOP sh
                  ON sh.BRD_CD = d.BRD_CD AND sh.SHOP_ID = d.SHOP_ID
                LEFT JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_STOCK_SCHEMA}.DB_PRDT p
                  ON d.PRDT_CD = p.PRDT_CD
                WHERE d.BRD_CD = %s AND d.PART_CD = %s
                  AND d.DT = ({latest_dt_sub})
                  {shop_filter}
                GROUP BY d.COLOR_CD
                ORDER BY WH_STOCK DESC
            """, (ec_logistics_shop_id, stock_brand_cd, part_cd, ec_logistics_shop_id))
        stock_rows = cursor.fetchall()

        # 최근 7일 자사몰(온라인쇼핑몰 직영) 판매량 (DW_SH_SCS_D, SHOP_ID=자사몰)
        color_filter = "AND COLOR_CD = %s" if color_cd else ""
        sale_params = [stock_brand_cd, jasamol_shop_id, part_cd] + ([color_cd] if color_cd else [])
        cursor.execute(f"""
            SELECT SUM(SALE_NML_QTY - SALE_RET_QTY) AS SALE_QTY
            FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_STOCK_SCHEMA}.DW_SH_SCS_D
            WHERE BRD_CD = %s
              AND SHOP_ID = %s
              AND PART_CD = %s
              {color_filter}
              AND DT >= CURRENT_DATE - 7
              AND DT <  CURRENT_DATE
        """, sale_params)
        sale_row = cursor.fetchone()
        sale_7d   = int(sale_row[0] or 0) if sale_row else 0
        daily_avg = round(sale_7d / 7, 1)

        # 매장 유형별 7일 판매 (백화점 AX+AS / 대리점 S2 / 직영점 S1)
        offline_filter = "AND COLOR_CD = %s" if color_cd else ""
        offline_params = [stock_brand_cd, part_cd] + ([color_cd] if color_cd else [])
        cursor.execute(f"""
            SELECT shop_type,
                   COUNT(DISTINCT shop_id)        AS shops,
                   SUM(net_qty)                   AS sale_7d
            FROM (
                SELECT d.SHOP_ID AS shop_id,
                       (d.SALE_NML_QTY - d.SALE_RET_QTY) AS net_qty,
                       CASE WHEN s.ANLYS_DIST_TYPE_CD IN ('AX','AS') THEN '백화점'
                            WHEN s.ANLYS_DIST_TYPE_CD = 'S2'         THEN '대리점'
                            WHEN s.ANLYS_DIST_TYPE_CD = 'S1'         THEN '직영점'
                            ELSE '기타' END AS shop_type
                FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_STOCK_SCHEMA}.DW_SH_SCS_D d
                JOIN {SNOWFLAKE_DATABASE}.{SNOWFLAKE_STOCK_SCHEMA}.DB_SHOP s
                  ON s.BRD_CD = d.BRD_CD AND s.SHOP_ID = d.SHOP_ID
                WHERE d.BRD_CD = %s AND d.PART_CD = %s
                  {offline_filter}
                  AND d.DT >= CURRENT_DATE - 7 AND d.DT < CURRENT_DATE
                  AND s.ANLYS_DIST_TYPE_CD IN ('AX','AS','S2','S1')
            )
            GROUP BY shop_type
        """, offline_params)
        offline_rows = cursor.fetchall()
        offline_sales = {
            r[0]: {"shops": int(r[1] or 0), "sale_7d": int(r[2] or 0),
                   "daily_avg": round((r[2] or 0) / 7, 1)}
            for r in offline_rows
        }
        conn.close()

        if not stock_rows:
            return None

        # LEFT JOIN 후 PRDT_NM이 NULL인 행이 있을 수 있으므로 첫 번째 non-null 값 사용
        prdt_nm = next((r[3] for r in stock_rows if r[3]), "") or ""
        is_mc   = "MC" in prdt_nm.upper().split()

        if color_cd:
            # 사이즈별
            sizes = [{"size": r[0], "wh": int(r[1] or 0), "total": int(r[2] or 0)} for r in stock_rows]
            sizes.sort(key=lambda x: _SIZE_ORDER.get(x["size"], 99))
            total_wh    = sum(s["wh"] for s in sizes)
            total_all   = sum(s["total"] for s in sizes)
            offline_stock = max(total_all - total_wh, 0)
            days_of_supply = round(total_wh / daily_avg, 0) if daily_avg > 0 else None
            o2o_signal     = _o2o_signal(days_of_supply, offline_stock, daily_avg, stock_brand_cd)
            return {
                "prdt_nm":        prdt_nm,
                "brand_cd":       stock_brand_cd,
                "is_mc":          is_mc,
                "sizes":          sizes,
                "sale_7d":        sale_7d,
                "daily_avg":      daily_avg,
                "days_of_supply": days_of_supply,
                "offline_stock":  offline_stock,
                "offline_sales":  offline_sales,
                "o2o_signal":     o2o_signal,
            }
        else:
            # 컬러별
            colors = [{"color": r[0], "wh": int(r[1] or 0), "total": int(r[2] or 0)} for r in stock_rows]
            total_wh    = sum(c["wh"] for c in colors)
            total_all   = sum(c["total"] for c in colors)
            offline_stock = max(total_all - total_wh, 0)
            days_of_supply = round(total_wh / daily_avg, 0) if daily_avg > 0 else None
            o2o_signal     = _o2o_signal(days_of_supply, offline_stock, daily_avg, stock_brand_cd)
            return {
                "prdt_nm":        prdt_nm,
                "brand_cd":       stock_brand_cd,
                "is_mc":          is_mc,
                "colors":         colors,
                "sale_7d":        sale_7d,
                "daily_avg":      daily_avg,
                "days_of_supply": days_of_supply,
                "offline_stock":  offline_stock,
                "offline_sales":  offline_sales,
                "o2o_signal":     o2o_signal,
            }
    except Exception as e:
        print(f"[경고] 재고 조회 실패 ({part_cd}-{color_cd}): {e}")
        return None


def _stock_items(stock_info_item):
    """sizes 또는 colors 리스트를 반환."""
    return stock_info_item.get("sizes") or stock_info_item.get("colors") or []


def _status_badge_info(dos, total_wh, brand_cd: str = "M"):
    """(상태 텍스트, 배경색) 반환. brand_cd 기준 DOS_CUTOFFS 적용."""
    if total_wh == 0:
        return "즉시", "#c0392b"
    cutoffs = _dos_cutoffs(brand_cd)
    if dos is not None and dos < cutoffs["urgent"]:
        return "긴급", "#e74c3c"
    elif dos is not None and dos < cutoffs["warning"]:
        return "주의", "#f39c12"
    else:
        return "안정", "#27ae60"


def _format_color_breakdown(item) -> str:
    """컬러별 물류재고 한줄 포맷: WHS 50개 / INL 30개 / ..."""
    colors = item.get("colors", [])
    return " / ".join(f"{c['color']} {c['wh']}개" for c in colors if c["wh"] > 0) or "재고 없음"


def format_stock_summary(stock_info) -> str:
    """퍼마용: 물류재고 합계 + 가이드. MC 제품은 잔여 재고만 표기. 멀티 상품은 합산."""
    if not stock_info:
        return "재고 정보 없음"
    # 멀티 상품 리스트인 경우 각각 요약
    if isinstance(stock_info, list):
        parts = []
        for s in stock_info:
            nm = s.get("prdt_nm", "?") or "?"
            if s.get("colors") is not None:
                # 컬러별 표기
                parts.append(f"{nm}: {_format_color_breakdown(s)}")
            else:
                total_wh = sum(sz["wh"] for sz in _stock_items(s))
                parts.append(f"{nm}: {total_wh}개")
        return "\n".join(parts)
    items = _stock_items(stock_info)
    total_wh  = sum(s["wh"] for s in items)
    total_all = sum(s["total"] for s in items)
    prdt_nm   = stock_info.get("prdt_nm") or ""
    prefix    = f"{prdt_nm}: " if prdt_nm else ""

    if stock_info.get("is_mc"):
        if total_all == 0:
            return f"{prefix}[MC 한정] 완판 · 예산 다른 소재로 이동"
        elif total_wh == 0:
            return f"{prefix}[MC 한정] 온라인 완판 · 매장 재고 {total_all:,}개 잔여"
        else:
            return f"{prefix}[MC 한정] 온라인재고 {total_wh:,}개 · 소진 유도 · 예산 유지 또는 소폭 증액"

    dos       = stock_info.get("days_of_supply")
    daily_avg = stock_info.get("daily_avg", 0)
    sale_7d   = stock_info.get("sale_7d", 0)
    offline   = stock_info.get("offline_stock", 0)
    o2o       = stock_info.get("o2o_signal")
    cutoffs   = _dos_cutoffs(stock_info.get("brand_cd", "M"))
    sale_info = f"자사몰 7일 {sale_7d}개 · 일평균 {daily_avg}개" if sale_7d else "자사몰 판매 없음"
    if total_wh == 0:
        guide = f"온라인재고 소진 · 광고 중단 검토 · {sale_info}"
    elif o2o == "URGENT":
        guide = f"🔄[재배치 시급] 온라인 ~{int(dos)}일치 · 매장 적체 · 매장→자사몰 즉시 이동 · {sale_info}"
    elif o2o == "SUGGEST":
        guide = f"🔄[재배치 권장] 온라인 ~{int(dos)}일치 · 매장 여유 · 매장→자사몰 이동 검토 · {sale_info}"
    elif dos is not None and dos < cutoffs["urgent"]:
        guide = f"~{int(dos)}일치 · 단기 소진 예상 · 자사몰 물류 즉시 보충 필요 · {sale_info}"
    elif dos is not None and dos < cutoffs["warning"]:
        guide = f"~{int(dos)}일치 · 2주 내외 소진 예상 · 자사몰로 물류 이동 검토 · {sale_info}"
    else:
        dos_str = f"~{int(dos)}일치" if dos else "판매 없음"
        guide = f"재고 여유 · {dos_str} · 일cap 상향 검토 가능 · {sale_info}"
    breakdown = f"온라인재고 {total_wh:,}개 · 매장 {offline:,}개 · 전체 {total_all:,}개"
    return f"{prefix}{breakdown} · {guide}"


def format_stock_md_guide(stock_info) -> str:
    """MD용: 사이즈별 물류재고 + 가이드. MC 제품은 잔여 재고만 표기. 멀티 상품은 상품별 1줄씩."""
    if not stock_info:
        return ""
    # 멀티 상품 리스트인 경우 상품별 컬러/재고 + 액션 가이드
    def _action(total_wh, total_all, dos, sale_7d, daily_avg, brand_cd, offline_stock=0, o2o=None, offline_sales=None):
        sale_info = f"자사몰 7일 {sale_7d}개 · 일평균 {daily_avg}개" if sale_7d else "자사몰 판매 없음"
        offline_breakdown = ""
        if offline_sales:
            parts = [f"{k}({v['shops']}점) 일평균 {v['daily_avg']}개"
                     for k, v in offline_sales.items() if v.get('daily_avg', 0) > 0]
            if parts:
                offline_breakdown = " | " + " / ".join(parts)
        cutoffs = _dos_cutoffs(brand_cd)
        if total_wh == 0:
            return f"[즉시] 온라인재고 없음 (매장 재고 {total_all}개) → 광고 전환 대응 불가, 자사몰로 즉시 물류 이동 필요{offline_breakdown}"
        if o2o == "URGENT":
            return f"🔄[재배치 시급] 온라인 ~{int(dos)}일치 · 매장 합 {offline_stock:,}개 적체 → 매장→자사몰 즉시 이동 ({sale_info}){offline_breakdown}"
        if o2o == "SUGGEST":
            return f"🔄[재배치 권장] 온라인 ~{int(dos)}일치 · 매장 합 {offline_stock:,}개 여유 → 매장→자사몰 이동 검토 ({sale_info}){offline_breakdown}"
        if dos is not None and dos < cutoffs["urgent"]:
            return f"[긴급] ~{int(dos)}일치, {sale_info} → 자사몰 물류 즉시 보충 필수{offline_breakdown}"
        if dos is not None and dos < cutoffs["warning"]:
            return f"[주의] ~{int(dos)}일치, {sale_info} → 자사몰로 물류 이동 검토{offline_breakdown}"
        dos_str = f"~{int(dos)}일치" if dos else "판매 없음"
        return f"[안정] {dos_str}, {sale_info} → 광고 지속 운영 가능{offline_breakdown}"

    if isinstance(stock_info, list):
        lines = []
        for s in stock_info:
            nm        = s.get("prdt_nm", "")
            dos       = s.get("days_of_supply")
            sale_7d   = s.get("sale_7d", 0)
            daily_avg = s.get("daily_avg", 0)
            brand_cd  = s.get("brand_cd", "M")
            offline_stock = s.get("offline_stock", 0)
            o2o           = s.get("o2o_signal")
            offline_sales = s.get("offline_sales")
            if s.get("colors") is not None:
                total_wh   = sum(c["wh"] for c in s["colors"])
                total_all  = sum(c["total"] for c in s["colors"])
                action = _action(total_wh, total_all, dos, sale_7d, daily_avg, brand_cd, offline_stock, o2o, offline_sales)
                lines.append(f"{nm}: {action}")
            else:
                szs       = _stock_items(s)
                total_wh  = sum(sz["wh"] for sz in szs)
                total_all = sum(sz["total"] for sz in szs)
                action = _action(total_wh, total_all, dos, sale_7d, daily_avg, brand_cd, offline_stock, o2o, offline_sales)
                lines.append(f"{nm}: {action}")
        return "\n".join(lines)

    items    = _stock_items(stock_info)
    total_wh = sum(s["wh"] for s in items)
    dos       = stock_info.get("days_of_supply")
    sale_7d   = stock_info.get("sale_7d", 0)
    daily_avg = stock_info.get("daily_avg", 0)
    brand_cd  = stock_info.get("brand_cd", "M")
    offline_stock = stock_info.get("offline_stock", 0)
    o2o           = stock_info.get("o2o_signal")
    offline_sales = stock_info.get("offline_sales")

    # 컬러별 데이터인 경우 (color_cd 없이 조회된 단일 상품) — 색상별 재고는 재고 현황에 표시되므로 액션만 출력
    if stock_info.get("colors") is not None:
        total_all  = sum(c["total"] for c in stock_info["colors"])
        action = _action(total_wh, total_all, dos, sale_7d, daily_avg, brand_cd, offline_stock, o2o, offline_sales)
        return action

    sizes = items
    size_line = " / ".join(
        f"{s['size']} {s['wh']}개" + (f"(전체 {s['total']})" if s['total'] != s['wh'] else "")
        for s in sizes
    )

    # MC 제품: 잔여 재고 + 소진 목표 가이드
    if stock_info.get("is_mc"):
        total_all_mc = sum(s["total"] for s in sizes)
        if total_all_mc == 0:
            return f"{size_line}\n완판 완료 → 광고 중단"
        elif total_wh == 0:
            return f"{size_line}\n온라인재고 소진 (매장 재고 {total_all_mc}개 잔여) → 광고 중단 검토"
        else:
            return f"{size_line}\n잔여 {total_wh}개 · 소진 목표 유지 → 광고 지속"

    total_all     = sum(s["total"] for s in sizes)
    zero_wh_sizes = [s["size"] for s in sizes if s["wh"] == 0 and s["total"] > 0]
    action = _action(total_wh, total_all, dos, sale_7d, daily_avg, brand_cd, offline_stock, o2o, offline_sales)

    if zero_wh_sizes:
        action += f" | {', '.join(zero_wh_sizes)} 온라인재고 없음 주의"

    return f"{size_line}\n{action}"


def build_stock_html(stock_info) -> str:
    """재고 현황 + MD 액션 가이드를 HTML 테이블로 반환."""
    if not stock_info:
        return ""

    # ── 공통 스타일 ──
    TH_GREEN = (
        'style="padding:6px 10px;border:1px solid #c8e6c9;background:#e8f5e9;'
        'text-align:left;font-size:12px;color:#2e7d32;white-space:nowrap;"'
    )
    TH_GREEN_R = (
        'style="padding:6px 10px;border:1px solid #c8e6c9;background:#e8f5e9;'
        'text-align:right;font-size:12px;color:#2e7d32;white-space:nowrap;"'
    )
    TD_G  = 'style="padding:6px 10px;border:1px solid #c8e6c9;font-size:13px;color:#333;"'
    TD_GR = 'style="padding:6px 10px;border:1px solid #c8e6c9;font-size:13px;color:#333;text-align:right;"'
    ROW_ALT_G = 'style="background:#f9fbe7;"'

    TH_ORG = (
        'style="padding:6px 10px;border:1px solid #ffe0b2;background:#fff3e0;'
        'text-align:left;font-size:12px;color:#e65100;white-space:nowrap;"'
    )
    TH_ORG_C = (
        'style="padding:6px 10px;border:1px solid #ffe0b2;background:#fff3e0;'
        'text-align:center;font-size:12px;color:#e65100;white-space:nowrap;"'
    )
    TH_ORG_R = (
        'style="padding:6px 10px;border:1px solid #ffe0b2;background:#fff3e0;'
        'text-align:right;font-size:12px;color:#e65100;white-space:nowrap;"'
    )
    TD_O  = 'style="padding:6px 10px;border:1px solid #ffe0b2;font-size:12px;color:#333;"'
    TD_OC = 'style="padding:6px 10px;border:1px solid #ffe0b2;font-size:12px;color:#333;text-align:center;"'
    TD_OR = 'style="padding:6px 10px;border:1px solid #ffe0b2;font-size:12px;color:#333;text-align:right;"'
    ROW_ALT_O = 'style="background:#fff8f0;"'

    def _action_text(total_wh, total_all, dos, brand_cd, offline_stock=0, o2o=None):
        if total_wh == 0:
            return f"온라인재고 없음 (매장 {total_all:,}개) → 즉시 물류 이동 필요"
        if o2o == "URGENT":
            return f"🔄 [재배치 시급] 매장 {offline_stock:,}개 적체 → 매장→자사몰 즉시 이동"
        if o2o == "SUGGEST":
            return f"🔄 [재배치 권장] 매장 {offline_stock:,}개 여유 → 매장→자사몰 이동 검토"
        cutoffs = _dos_cutoffs(brand_cd)
        if dos is not None and dos < cutoffs["urgent"]:
            return "광고 전환 증가 예상 → 자사몰 물류 즉시 보충 필수"
        elif dos is not None and dos < cutoffs["warning"]:
            return "단기 소진 예상 → 자사몰 물류 이동 검토"
        else:
            return "광고 지속 운영 가능"

    def _offline_breakdown(offline_sales):
        """매장 유형별 일평균을 한 셀에 표시: '백 43 / 대 19 / 직 20'."""
        if not offline_sales:
            return "-"
        parts = []
        for key, short in [("백화점", "백"), ("대리점", "대"), ("직영점", "직")]:
            v = offline_sales.get(key)
            if v and v.get("daily_avg", 0) > 0:
                parts.append(f"{short} {v['daily_avg']:.1f}({v['shops']}점)")
        return " / ".join(parts) if parts else "-"

    def _badge(status, color):
        return (
            f'<span style="background:{color};color:#fff;padding:2px 8px;'
            f'border-radius:3px;font-size:11px;font-weight:bold;">{status}</span>'
        )

    # ── 재고 현황 테이블 ──
    if isinstance(stock_info, list):
        stock_rows = ""
        for i, s in enumerate(stock_info):
            nm = s.get("prdt_nm", "?") or "?"
            bg = ROW_ALT_G if i % 2 == 1 else ""
            if s.get("colors") is not None:
                total_wh  = sum(c["wh"]    for c in s["colors"])
                breakdown = " / ".join(f'{c["color"]} {c["wh"]:,}' for c in s["colors"] if c["wh"] > 0) or "재고 없음"
            else:
                szs      = _stock_items(s)
                total_wh = sum(sz["wh"] for sz in szs)
                breakdown = " / ".join(f'{sz["size"]} {sz["wh"]:,}' for sz in szs if sz["wh"] > 0) or "재고 없음"
            stock_rows += (
                f'<tr {bg}>'
                f'<td {TD_G}>{nm}</td>'
                f'<td {TD_G}>{breakdown}</td>'
                f'<td {TD_GR}>{total_wh:,}개</td>'
                f'</tr>'
            )
        stock_table = (
            '<table style="border-collapse:collapse;width:100%;font-size:13px;">'
            f'<thead><tr><th {TH_GREEN}>상품명</th><th {TH_GREEN}>컬러/사이즈별 온라인재고</th><th {TH_GREEN_R}>합계</th></tr></thead>'
            f'<tbody>{stock_rows}</tbody></table>'
        )
    elif stock_info.get("colors") is not None:
        colors = stock_info["colors"]
        stock_rows = ""
        for i, c in enumerate(colors):
            bg = ROW_ALT_G if i % 2 == 1 else ""
            stock_rows += (
                f'<tr {bg}>'
                f'<td {TD_G}>{c["color"]}</td>'
                f'<td {TD_GR}>{c["wh"]:,}개</td>'
                f'<td {TD_GR}>{c["total"]:,}개</td>'
                f'</tr>'
            )
        stock_table = (
            '<table style="border-collapse:collapse;width:100%;font-size:13px;">'
            f'<thead><tr><th {TH_GREEN}>컬러</th><th {TH_GREEN_R}>온라인재고</th><th {TH_GREEN_R}>전체재고</th></tr></thead>'
            f'<tbody>{stock_rows}</tbody></table>'
        )
    else:
        sizes = _stock_items(stock_info)
        stock_rows = ""
        for i, s in enumerate(sizes):
            bg = ROW_ALT_G if i % 2 == 1 else ""
            stock_rows += (
                f'<tr {bg}>'
                f'<td {TD_G}>{s["size"]}</td>'
                f'<td {TD_GR}>{s["wh"]:,}개</td>'
                f'<td {TD_GR}>{s["total"]:,}개</td>'
                f'</tr>'
            )
        stock_table = (
            '<table style="border-collapse:collapse;width:100%;font-size:13px;">'
            f'<thead><tr><th {TH_GREEN}>사이즈</th><th {TH_GREEN_R}>온라인재고</th><th {TH_GREEN_R}>전체재고</th></tr></thead>'
            f'<tbody>{stock_rows}</tbody></table>'
        )

    # ── MD 액션 가이드 테이블 ──
    md_header = (
        f'<thead><tr>'
        f'<th {TH_ORG}>상품명</th>'
        f'<th {TH_ORG_C}>상태</th>'
        f'<th {TH_ORG_R}>재고일수</th>'
        f'<th {TH_ORG_R}>자사몰<br/>7일 판매</th>'
        f'<th {TH_ORG_R}>자사몰<br/>일평균</th>'
        f'<th {TH_ORG}>매장 일평균<br/>(백/대/직)</th>'
        f'<th {TH_ORG}>액션</th>'
        f'</tr></thead>'
    )

    def _md_row(nm, dos, sale_7d, daily_avg, total_wh, total_all, bg, brand_cd,
                offline_stock=0, o2o=None, offline_sales=None):
        status, color = _status_badge_info(dos, total_wh, brand_cd)
        dos_str  = f"~{int(dos)}일" if dos else "-"
        sale_str = f"{int(sale_7d):,}개"  if sale_7d  else "-"
        avg_str  = f"{daily_avg:.1f}개"   if daily_avg else "-"
        offline_cell = _offline_breakdown(offline_sales)
        action   = _action_text(total_wh, total_all, dos, brand_cd, offline_stock, o2o)
        return (
            f'<tr {bg}>'
            f'<td {TD_O}>{nm}</td>'
            f'<td {TD_OC}>{_badge(status, color)}</td>'
            f'<td {TD_OR}>{dos_str}</td>'
            f'<td {TD_OR}>{sale_str}</td>'
            f'<td {TD_OR}>{avg_str}</td>'
            f'<td {TD_O}>{offline_cell}</td>'
            f'<td {TD_O}>{action}</td>'
            f'</tr>'
        )

    if isinstance(stock_info, list):
        md_rows = ""
        for i, s in enumerate(stock_info):
            nm        = s.get("prdt_nm", "?") or "?"
            dos       = s.get("days_of_supply")
            sale_7d   = s.get("sale_7d", 0)
            daily_avg = s.get("daily_avg", 0)
            brand_cd  = s.get("brand_cd", "M")
            offline_stock = s.get("offline_stock", 0)
            o2o           = s.get("o2o_signal")
            offline_sales = s.get("offline_sales")
            if s.get("colors") is not None:
                total_wh  = sum(c["wh"]    for c in s["colors"])
                total_all = sum(c["total"] for c in s["colors"])
            else:
                szs       = _stock_items(s)
                total_wh  = sum(sz["wh"]    for sz in szs)
                total_all = sum(sz["total"] for sz in szs)
            bg = ROW_ALT_O if i % 2 == 1 else ""
            md_rows += _md_row(nm, dos, sale_7d, daily_avg, total_wh, total_all, bg, brand_cd,
                               offline_stock, o2o, offline_sales)
    else:
        dos       = stock_info.get("days_of_supply")
        sale_7d   = stock_info.get("sale_7d", 0)
        daily_avg = stock_info.get("daily_avg", 0)
        brand_cd  = stock_info.get("brand_cd", "M")
        offline_stock = stock_info.get("offline_stock", 0)
        o2o           = stock_info.get("o2o_signal")
        offline_sales = stock_info.get("offline_sales")
        nm        = stock_info.get("prdt_nm", "") or ""
        if stock_info.get("colors") is not None:
            total_wh  = sum(c["wh"]    for c in stock_info["colors"])
            total_all = sum(c["total"] for c in stock_info["colors"])
        else:
            szs       = _stock_items(stock_info)
            total_wh  = sum(sz["wh"]    for sz in szs)
            total_all = sum(sz["total"] for sz in szs)
        md_rows = _md_row(nm, dos, sale_7d, daily_avg, total_wh, total_all, "", brand_cd,
                          offline_stock, o2o, offline_sales)

    md_table = (
        '<table style="border-collapse:collapse;width:100%;font-size:13px;">'
        f'{md_header}<tbody>{md_rows}</tbody></table>'
    )

    return (
        '<div style="margin-top:12px;padding:12px;background:#f1f8e9;'
        'border-left:4px solid #558b2f;border-radius:4px;">'
        '<p style="margin:0 0 8px;font-size:11px;color:#2e7d32;font-weight:bold;">📦 재고 현황</p>'
        + stock_table +
        '<p style="margin:14px 0 8px;font-size:11px;color:#e65100;font-weight:bold;">MD 액션 가이드</p>'
        + md_table +
        '</div>'
    )


def detect_channel(campaign_name: str, adset_name: str) -> str:
    text = f"{campaign_name or ''} {adset_name or ''}".lower()
    if "무신사" in text or "musinsa" in text:
        return "MUSINSA"
    return "OFFICIAL"


def determine_action_type(roas_6h: float, spend_6h: float, purchases_6h: float, brand: str = "MLB") -> str | None:
    """우선순위 순으로 action_type 결정. 해당 없으면 None."""
    cs = _get_action_cond(brand, "CAMPAIGN_SCALE")
    pe = _get_action_cond(brand, "PRODUCT_EXTRACTION")
    ce = _get_action_cond(brand, "CREATIVE_EXPANSION")
    if roas_6h >= cs["roas_6h_min"] and purchases_6h >= cs["purchases_6h_min"]:
        return "CAMPAIGN_SCALE"
    if roas_6h >= pe["roas_6h_min"] and spend_6h >= pe["spend_6h_min"]:
        return "PRODUCT_EXTRACTION"
    if roas_6h >= ce["roas_6h_min"] and purchases_6h >= ce["purchases_6h_min"]:
        return "CREATIVE_EXPANSION"
    return None


def determine_alert_subtype(
    ctr_6h: float, ctr_12h: float,
    purchases_6h: float, roas_6h: float, roas_12h: float,
    purchases_prev_6h: float = 0, clicks_6h: float = 0,
    clicks_prev_6h: float = 0, roas_prev_6h: float = 0,
    purchases_12h: float = 0,
    brand: str = "MLB",
) -> str:
    """
    alert 성격 분류 (전환 단계 기준)
    CONVERSION_SURGE_COLD: 최근 6h 전환 >= 3건, clicks >= 100, 직전 6h 전환 == 0건 (첫 발생)
    CONVERSION_SURGE:      최근 6h 전환 >= 3건, clicks >= 100, CVR/ROAS 모두 직전 6h 대비 개선
                           또는 최근 6h 전환 2건이지만 12h 누적 >= 3건 + ROAS >= 기준치 (12h 누적 전환형)
    CONVERSION_EARLY:      전환 == 2건, ROAS >= 기준치 (초기 전환 감지형)
    CLICK_TO_CONVERT_GAP:  전환 == 2건, CTR 상승 + ROAS < 기준치 (전환 미흡형)
    CLICK_SURGE:           전환 0건, CTR_6h > CTR_12h (순수 클릭 반응형)
    DEFAULT:               위 조건에 해당하지 않는 경우
    """
    roas_threshold = _get_opp_filter(brand)["roas_6h_min"]

    # purchases >= 3: Winner 판단
    if purchases_6h >= 3 and clicks_6h >= 100:
        if purchases_prev_6h == 0:
            return "CONVERSION_SURGE_COLD"
        cvr_recent = purchases_6h      / clicks_6h      if clicks_6h      > 0 else 0
        cvr_prev   = purchases_prev_6h / clicks_prev_6h if clicks_prev_6h > 0 else 0
        if (
            (purchases_6h - purchases_prev_6h) >= 1
            and roas_6h > roas_prev_6h
            and cvr_recent > cvr_prev
        ):
            return "CONVERSION_SURGE"

    # purchases == 2: 초기 전환 단계
    if purchases_6h == 2:
        # 12시간 누적 전환이 3건 이상이면 CONVERSION_SURGE에 준하는 대응
        if purchases_12h >= 3 and roas_6h >= roas_threshold:
            return "CONVERSION_SURGE"
        if roas_6h >= roas_threshold:
            return "CONVERSION_EARLY"
        if ctr_6h > ctr_12h:
            return "CLICK_TO_CONVERT_GAP"

    # purchases 0: 순수 클릭 반응
    if purchases_6h == 0 and ctr_6h > ctr_12h:
        return "CLICK_SURGE"

    return "DEFAULT"


def determine_br_subtype(ctr_6h: float, ctr_12h: float, brand: str = "MLB") -> str | None:
    """
    BR(브랜딩) 캠페인 전용 subtype 판정. 브랜드별 surge/drop ratio 적용.
    CTR_SURGE: ctr_6h >= ctr_12h * surge_ratio
    CTR_DROP:  ctr_6h <= ctr_12h * drop_ratio
    None:      조건 미해당
    """
    if ctr_12h <= 0:
        return None
    c = _get_br_cond(brand)
    if ctr_6h >= ctr_12h * c["ctr_surge_ratio"]:
        return "BR_CTR_SURGE"
    if ctr_6h <= ctr_12h * c["ctr_drop_ratio"]:
        return "BR_CTR_DROP"
    return None


# ─────────────────────────────────────────
# 중복 발송 방지 + repeat_count (로컬 JSON)
# ─────────────────────────────────────────
def load_alert_log() -> dict:
    if not os.path.exists(ALERT_LOG_FILE):
        return {}
    with open(ALERT_LOG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_alert_log(log: dict) -> None:
    with open(ALERT_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)


def _alert_log_key(ad_id: str, brand: str) -> str:
    """두 브랜드가 META 계정을 공유하므로 ad_id는 글로벌 유니크지만,
    안전하게 brand prefix를 붙여 키 충돌·교차 영향을 차단한다."""
    return f"{brand}:{ad_id}"


def is_recently_alerted(ad_id: str, brand: str, hours: int = 12) -> bool:
    log = load_alert_log()
    entry = log.get(_alert_log_key(ad_id, brand), {})
    last_sent_str = entry.get("last_sent") if isinstance(entry, dict) else entry
    if not last_sent_str:
        return False
    last_sent = datetime.fromisoformat(last_sent_str)
    return datetime.now(timezone.utc) - last_sent < timedelta(hours=hours)


def get_repeat_count(ad_id: str, brand: str, days: int = 7) -> int:
    log = load_alert_log()
    entry = log.get(_alert_log_key(ad_id, brand), {})
    if not isinstance(entry, dict):
        return 1 if entry else 0
    history = entry.get("history", [])
    cutoff  = datetime.now(timezone.utc) - timedelta(days=days)
    return sum(1 for ts in history if datetime.fromisoformat(ts) >= cutoff)


def mark_alert_sent(ad_id: str, brand: str) -> None:
    log   = load_alert_log()
    now   = datetime.now(timezone.utc).isoformat()
    key   = _alert_log_key(ad_id, brand)
    entry = log.get(key, {})
    if not isinstance(entry, dict):
        entry = {}
    history = entry.get("history", [])
    history.append(now)
    # 최근 30일 이력만 유지
    cutoff  = datetime.now(timezone.utc) - timedelta(days=30)
    history = [ts for ts in history if datetime.fromisoformat(ts) >= cutoff]
    log[key] = {"last_sent": now, "history": history}
    save_alert_log(log)


# ─────────────────────────────────────────
# Gemini AI 인사이트
# ─────────────────────────────────────────
# FALLBACK은 alert_subtype 기준으로 분기 (ASC 구조 전제)
FALLBACK = {
    # ── Performance 알럿 ──
    "CLICK_SURGE": (
        "클릭 반응이 급증한 소재로 썸네일·카피 반응이 좋은 구간입니다. 다만 전환은 아직 발생하지 않은 상태입니다.",
        "썸네일·카피 컨셉을 유지하고 랜딩 페이지 및 상품 적합성을 점검하세요. 전환 유도형 카피/오퍼 변형 테스트를 권장합니다.",
    ),
    "CONVERSION_EARLY": (
        "전환이 발생하며 ROAS가 기준치를 상회하는 초기 전환 신호입니다. 모수는 작지만 전환 가능성이 확인된 구간입니다.",
        "6시간 추가 관찰하여 ROAS 유지 여부를 확인하세요. 동일 썸네일·카피 컨셉 기반 유사 소재 2~3종 제작을 권장하며, ASC 캠페인 일cap 상향도 검토하세요.",
    ),
    "CLICK_TO_CONVERT_GAP": (
        "클릭 반응과 일부 전환은 발생했으나 ROAS가 기준치에 미달하는 구간입니다. 유입 대비 전환 효율이 낮은 상태입니다.",
        "상세페이지, 가격, 혜택 등 전환 요소를 점검하세요. 썸네일 유지 + 카피/오퍼 중심으로 수정 테스트 후 재검증하세요.",
    ),
    "CONVERSION_SURGE": (
        "직전 6시간 대비 전환율과 ROAS가 모두 개선된 실질적 전환 급증 구간입니다.",
        "ASC 캠페인 일cap 상향을 검토하고, 해당 소재 내 상품 기반 전환형 신규 소재 2~3종 추가 제작을 권장합니다.",
    ),
    "CONVERSION_SURGE_COLD": (
        "직전 6시간 전환이 없다가 최근 6시간 내 첫 전환이 발생한 구간입니다. 지속성은 추가 관찰이 필요합니다.",
        "첫 전환 발생 구간이므로 일cap 상향은 보류하고, 해당 소재와 상품을 메모해두고 다음 6시간 추이를 확인하세요.",
    ),
    "DEFAULT": (
        "해당 소재의 6시간 성과가 기준치를 초과하여 기회 구간으로 판단됩니다.",
        "캠페인 일cap 상향을 검토하고, 해당 소재 내 상품으로 신규 소재 2~3종 추가 제작을 권장합니다.",
    ),
    # ── BR(브랜딩) 알럿 ──
    "BR_CTR_SURGE": (
        "브랜딩 캠페인의 CTR이 직전 대비 상승한 구간입니다. 썸네일·카피 반응이 좋아지고 있습니다.",
        "반응이 좋은 이 소재의 썸네일·카피 컨셉을 기반으로 유사 소재 2~3종 추가 제작을 권장합니다.",
    ),
    "BR_CTR_DROP": (
        "브랜딩 캠페인의 CTR이 직전 대비 20% 이상 하락한 구간입니다. 소재 피로도를 확인하세요.",
        "CTR이 하락한 소재는 새로운 썸네일·카피 컨셉으로 소재를 교체하거나 신규 소재 투입을 검토하세요.",
    ),
}


def generate_ai_insight(alert: dict) -> tuple[str, str]:
    action_type   = alert["action_type"]
    alert_subtype = alert.get("alert_subtype", "DEFAULT")
    fallback      = FALLBACK.get(alert_subtype, FALLBACK["DEFAULT"])

    _ad_name       = alert.get("ad_name", "")
    is_partnership = "partner" in _ad_name.lower()
    is_influencer  = "인플루언서" in _ad_name and not is_partnership

    if not _gemini_client:
        return fallback

    is_br = alert.get("alert_type") == "BR"

    # subtype별 Gemini AI_INSIGHT 작성 지침 (왜 반응이 좋아졌는지만)
    subtype_context = {
        "CLICK_SURGE":          "CTR이 급등한 이유를 썸네일·카피 반응 관점에서 해석하세요. 전환은 아직 0건임을 반영하세요.",
        "CONVERSION_EARLY":     "소량이지만 전환이 발생하고 ROAS가 기준치를 상회하는 이유를 데이터 기반으로 해석하세요.",
        "CLICK_TO_CONVERT_GAP": "클릭은 발생했지만 전환 효율이 낮은 이유를 랜딩·상품·오퍼 관점에서 해석하세요.",
        "CONVERSION_SURGE":     "직전 6시간 대비 전환율과 ROAS가 모두 개선된 이유를 데이터 기반으로 해석하세요.",
        "CONVERSION_SURGE_COLD":"직전 6시간 전환이 없다가 첫 전환이 발생한 이유를 소재/상품 관점에서 해석하세요.",
        "DEFAULT":              "성과 개선 요인을 데이터 기반으로 해석하세요.",
        "BR_CTR_SURGE":         "브랜딩 소재의 CTR이 상승한 이유를 썸네일·카피 반응 관점에서 해석하세요. 전환 지표는 언급하지 마세요.",
        "BR_CTR_DROP":          "브랜딩 소재의 CTR이 하락한 이유를 소재 피로도·노출 포화 관점에서 해석하세요. 전환 지표는 언급하지 마세요.",
    }
    if is_partnership:
        subtype_context = {k: "파트너십 소재(인플루언서가 직접 게재한 협찬 콘텐츠)의 반응이 좋아진 이유를 소재 특성·타겟 공감 관점에서 해석하세요." for k in subtype_context}
    elif is_influencer:
        subtype_context = {k: "인플루언서컷을 활용해 제작한 광고 소재의 반응이 좋아진 이유를 소재 특성·비주얼 몰입도 관점에서 해석하세요." for k in subtype_context}

    if is_br:
        prompt = f"""
당신은 디지털 광고 브랜딩 마케터입니다.
아래 Meta 브랜딩 광고 데이터를 보고 AI 인사이트(왜 CTR 변화가 발생했는지)만 작성하세요.
전환(구매), ROAS, 매출 관련 내용은 절대 언급하지 마세요.

[광고 정보]
- 캠페인: {alert['campaign_name']}
- 광고소재: {alert['ad_name']}
- alert 유형: {alert_subtype}

[성과 데이터]
- Impressions_6h: {int(alert.get('impressions_6h', 0)):,}회
- Clicks_6h: {int(alert.get('clicks_6h', 0)):,}회
- CTR_6h: {alert.get('ctr_6h', 0):.2%} / CTR_12h: {alert.get('ctr_12h', 0):.2%}

[작성 지침]
- {subtype_context.get(alert_subtype, subtype_context['BR_CTR_SURGE'])}
- 입력 데이터만 근거로 해석, 외부 요인 추정 금지
- 숫자 과장 금지, 한국어, 짧고 실무적인 톤

[출력 형식] (반드시 아래 형식 그대로)
AI_INSIGHT: (한 문장)
""".strip()
    else:
        prompt = f"""
당신은 디지털 광고 퍼포먼스 마케터입니다.
아래 Meta 광고 데이터를 보고 AI 인사이트(왜 반응이 좋아졌는지)만 한 문장으로 작성하세요.

[광고 정보]
- 캠페인: {alert['campaign_name']}
- 광고소재: {alert['ad_name']}
- alert 유형: {alert_subtype} / {action_type}

[성과 데이터]
- Spend_6h: {alert['spend_6h']:,.0f}원
- Clicks_6h: {int(alert.get('clicks_6h', 0))}회
- Purchases_6h: {int(alert['purchases_6h'])}건
- Revenue_6h: {alert['revenue_6h']:,.0f}원
- ROAS_6h: {alert['roas_6h']:.1%} / ROAS_12h: {alert['roas_12h']:.1%}
- CTR_6h: {alert.get('ctr_6h', 0):.2%} / CTR_12h: {alert.get('ctr_12h', 0):.2%}

[작성 지침]
- {subtype_context.get(alert_subtype, subtype_context['DEFAULT'])}
- 입력 데이터만 근거로 해석, 외부 요인 추정 금지
- 숫자 과장 금지, 한국어, 짧고 실무적인 톤

[출력 형식] (반드시 아래 형식 그대로)
AI_INSIGHT: (한 문장)
""".strip()

    try:
        response = _gemini_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
        )
        text    = response.text.strip()
        insight = fallback[0]
        for line in text.splitlines():
            if line.startswith("AI_INSIGHT:"):
                insight = line.replace("AI_INSIGHT:", "").strip()
        return insight, fallback[1]
    except Exception as e:
        print(f"[경고] Gemini 호출 실패 ({e}) - fallback 사용")
        return fallback


def build_action_guide(alert: dict, stock_info) -> str:
    """
    룰 기반 액션 가이드 생성.
    ① 재고 경고 (stock_info 기반)
    ② 소재 액션 (파트너십 여부 분기)
    ③ 일cap 상향 (재고 여유 + 전환 충분할 때)
    """
    alert_subtype  = alert.get("alert_subtype", "DEFAULT")
    action_type    = alert.get("action_type", "")
    ad_name        = alert.get("ad_name", "")
    is_partnership = "partner" in ad_name.lower()
    is_influencer  = "인플루언서" in ad_name and not is_partnership
    is_br          = alert.get("alert_type") == "BR"

    # ── ① 재고 경고 ──
    stock_warn = ""
    if stock_info and not isinstance(stock_info, list):
        items    = _stock_items(stock_info) if not stock_info.get("colors") else stock_info["colors"]
        total_wh = sum(s["wh"] for s in items)
        dos      = stock_info.get("days_of_supply")
        offline  = stock_info.get("offline_stock", 0)
        o2o      = stock_info.get("o2o_signal")
        cutoffs  = _dos_cutoffs(stock_info.get("brand_cd", "M"))
        if total_wh == 0:
            stock_warn = "🚨 온라인재고 없음 → 광고 중단 후 자사몰 물류 이동 필요"
        elif o2o == "URGENT":
            stock_warn = f"🔄 [재배치 시급] 온라인 {int(dos)}일치 · 매장 합 {offline:,}개 적체 → 매장→자사몰 즉시 이동"
        elif o2o == "SUGGEST":
            stock_warn = f"🔄 [재배치 권장] 온라인 {int(dos)}일치 · 매장 합 {offline:,}개 여유 → 매장→자사몰 이동 검토"
        elif dos is not None and dos < cutoffs["urgent"]:
            stock_warn = f"⚠️ 재고 {int(dos)}일치 — 자사몰 물류 즉시 보충 필수"
        elif dos is not None and dos < cutoffs["warning"]:
            stock_warn = f"⚠️ 재고 {int(dos)}일치 — 자사몰로 물류 이동 검토"

    # ── ② 소재 액션 ──
    if is_br:
        if alert_subtype == "BR_CTR_DROP":
            creative_action = "새로운 썸네일·카피 컨셉으로 소재 교체 또는 신규 소재 투입"
        else:
            creative_action = "반응 좋은 소재 컨셉 기반 유사 소재 2~3종 추가 제작"
    elif is_partnership:
        creative_action = "해당 파트너십 소재 추가 확장"
    elif is_influencer:
        creative_action = "인플루언서 소재 추가 제작"
    else:
        if alert_subtype in ("CLICK_SURGE", "CLICK_TO_CONVERT_GAP"):
            creative_action = "랜딩 페이지·상품 상세 점검 및 전환 유도형 카피/오퍼 변형 테스트"
        else:
            creative_action = "해당 제품 기반 전환형 소재 2~3종 추가 제작"

    # ── ③ 일cap 상향 여부 ──
    cap_action = ""
    no_stock = (total_wh == 0) if (stock_info and not isinstance(stock_info, list)) else False
    if not is_br:
        if no_stock:
            cap_action = ""  # 재고 없으면 일cap 상향 보류
        elif alert_subtype == "CONVERSION_SURGE_COLD":
            cap_action = "다음 6h 추이 관찰 후 일cap 상향 검토"
        elif alert_subtype in ("CLICK_SURGE", "CLICK_TO_CONVERT_GAP"):
            cap_action = ""  # 전환 없거나 갭 있으면 보류
        elif action_type in ("CAMPAIGN_SCALE", "CONVERSION_SURGE", "CONVERSION_EARLY"):
            cap_action = "소재 일cap 상향"

    # ── ④ 재고 소진·긴급 시 광고 OFF ──
    stock_urgent = no_stock or (
        stock_info and not isinstance(stock_info, list)
        and stock_info.get("days_of_supply") is not None
        and stock_info["days_of_supply"] < _dos_cutoffs(stock_info.get("brand_cd", "M"))["urgent"]
    )
    off_action = "광고 OFF 검토" if stock_urgent else ""

    # ── 조합 ──
    actions = [a for a in [creative_action, cap_action, off_action] if a]
    action_line = " / ".join(f"{'①②③'[i]} {a}" for i, a in enumerate(actions))

    if stock_warn:
        return f"{stock_warn}\n→ {action_line}" if action_line else stock_warn
    return action_line


# ─────────────────────────────────────────
# 이메일 발송
# ─────────────────────────────────────────
ACTION_TYPE_KO = {
    "CAMPAIGN_SCALE":    "캠페인 예산 확장",
    "PRODUCT_EXTRACTION": "상품 분리 테스트",
    "CREATIVE_EXPANSION": "소재 확장",
}
ACTION_TYPE_COLOR = {
    "CAMPAIGN_SCALE":    "#1a73e8",
    "PRODUCT_EXTRACTION": "#e67e22",
    "CREATIVE_EXPANSION": "#27ae60",
}


def build_email_html(alerts: list, brand: str) -> str:
    now_kst = datetime.now(timezone.utc) + timedelta(hours=9)
    blocks  = ""

    for a in alerts:
        alert_subtype = a.get("alert_subtype", "DEFAULT")
        is_br         = a.get("alert_type") == "BR"
        repeat_label  = f"{a['repeat_count']}회" if a["repeat_count"] > 1 else "첫 발생"
        ctr_6h        = a.get("ctr_6h", 0)
        ctr_12h       = a.get("ctr_12h", 0)
        ctr_diff_pp   = (ctr_6h - ctr_12h) * 100
        clicks_6h     = int(a.get("clicks_6h", 0))

        # ── BR 브랜딩 알럿 블록 ──
        if is_br:
            br_color      = "#8e44ad" if alert_subtype == "BR_CTR_SURGE" else "#e74c3c"
            subtype_label = "CTR 상승형" if alert_subtype == "BR_CTR_SURGE" else "CTR 하락형"
            impressions_6h = int(a.get("impressions_6h", 0))
            ctr_diff_color = "#27ae60" if ctr_diff_pp >= 0 else "#e74c3c"
            blocks += f"""
        <div style="border:1px solid #e0e0e0;border-radius:8px;padding:20px;margin-bottom:24px;">
          <div style="display:flex;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:4px;">
            <span style="background:{br_color};color:#fff;padding:4px 10px;border-radius:4px;
                         font-size:12px;font-weight:bold;">BR 브랜딩</span>
            <span style="background:#6c757d;color:#fff;padding:3px 8px;border-radius:4px;
                         font-size:11px;font-weight:bold;margin-left:6px;">{subtype_label}</span>
            <span style="margin-left:auto;color:#999;font-size:12px;">최근 7일 {repeat_label}</span>
          </div>
          {(
              '<div style="margin-bottom:14px;text-align:center;">'
              f'<img src="{a["creative_image_url"]}" width="480" '
              'style="max-width:100%;border-radius:6px;border:1px solid #e0e0e0;" '
              'alt="소재 이미지" />'
              '</div>'
          ) if a.get("creative_image_url") else ""}
          <table style="width:100%;font-size:13px;border-collapse:collapse;margin-bottom:12px;">
            <tr><td style="padding:4px 8px;color:#888;width:110px;">Campaign</td>
                <td style="padding:4px 8px;font-family:monospace;font-size:12px;">{a['campaign_name']}</td></tr>
            <tr style="background:#f9f9f9;"><td style="padding:4px 8px;color:#888;">Ad Set</td>
                <td style="padding:4px 8px;font-family:monospace;font-size:12px;">{a['adset_name']}</td></tr>
            <tr><td style="padding:4px 8px;color:#888;">Creative</td>
                <td style="padding:4px 8px;font-family:monospace;font-size:12px;">{a['ad_name']}</td></tr>
            <tr style="background:#f9f9f9;"><td style="padding:4px 8px;color:#888;">Ad ID</td>
                <td style="padding:4px 8px;color:#555;font-size:12px;">{a['ad_id']}</td></tr>
          </table>
          <h4 style="margin:12px 0 8px;color:#333;font-size:13px;">최근 6시간 성과 (브랜딩 지표)</h4>
          <table style="border-collapse:collapse;width:100%;font-size:13px;">
            <thead>
              <tr style="background:#f0f4ff;">
                <th style="padding:6px 10px;border:1px solid #ddd;text-align:left;">지표</th>
                <th style="padding:6px 10px;border:1px solid #ddd;text-align:right;">현재값</th>
                <th style="padding:6px 10px;border:1px solid #ddd;text-align:right;">12h 대비</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td style="padding:6px 10px;border:1px solid #ddd;">Impressions_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{impressions_6h:,}회</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
              </tr>
              <tr style="background:#f9f9f9;">
                <td style="padding:6px 10px;border:1px solid #ddd;">Clicks_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{clicks_6h:,}회</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
              </tr>
              <tr>
                <td style="padding:6px 10px;border:1px solid #ddd;">CTR_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{ctr_6h:.2%}</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;
                    color:{ctr_diff_color};font-weight:bold;">{'+' if ctr_diff_pp >= 0 else ''}{ctr_diff_pp:.1f}%p</td>
              </tr>
              <tr style="background:#f9f9f9;">
                <td style="padding:6px 10px;border:1px solid #ddd;">CTR_12h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{ctr_12h:.2%}</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
              </tr>
            </tbody>
          </table>
          <div style="margin-top:10px;padding:12px;background:#f0f7ff;border-left:4px solid {br_color};border-radius:4px;">
            <p style="margin:0 0 4px;font-size:11px;color:#888;font-weight:bold;">AI 인사이트</p>
            <p style="margin:0;font-size:13px;color:#333;">{a['ai_insight']}</p>
          </div>
          <div style="margin-top:8px;padding:12px;background:#fff8e1;border-left:4px solid #f9a825;border-radius:4px;">
            <p style="margin:0 0 4px;font-size:11px;color:#888;font-weight:bold;">액션 가이드</p>
            <p style="margin:0;font-size:13px;color:#333;">{a['action_guide']}</p>
          </div>
          {build_stock_html(a.get("stock_info")) if a.get("stock_product") else ""}
        </div>
        """
            continue

        # ── Performance 알럿 블록 ──
        action_type   = a["action_type"]
        color         = ACTION_TYPE_COLOR.get(action_type, "#1a73e8")
        action_ko     = ACTION_TYPE_KO.get(action_type, action_type)

        # 기준치 계산
        ac          = _get_action_cond(brand, action_type)
        opp         = _get_opp_filter(brand)
        roas_base   = ac["roas_6h_min"]
        purch_base  = ac.get("purchases_6h_min", opp["purchases_6h_min"])
        spend_base  = ac.get("spend_6h_min",     opp["spend_6h_min"])
        roas_diff_pp  = (a["roas_6h"] - roas_base) * 100
        purch_diff    = int(a["purchases_6h"]) - purch_base

        # alert_subtype 뱃지 텍스트
        subtype_label = {
            "CLICK_SURGE":           "클릭 급증형",
            "CONVERSION_EARLY":      "초기 전환 감지형",
            "CLICK_TO_CONVERT_GAP":  "전환 미흡형",
            "CONVERSION_SURGE":      "전환 급증형",
            "CONVERSION_SURGE_COLD": "첫 전환 급등형",
            "DEFAULT":               "",
        }.get(alert_subtype, "")

        subtype_badge = (
            f'<span style="background:#6c757d;color:#fff;padding:3px 8px;border-radius:4px;'
            f'font-size:11px;font-weight:bold;margin-left:6px;">{subtype_label}</span>'
            if subtype_label else ""
        )

        blocks += f"""
        <div style="border:1px solid #e0e0e0;border-radius:8px;padding:20px;margin-bottom:24px;">
          <div style="display:flex;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:4px;">
            <span style="background:{color};color:#fff;padding:4px 10px;border-radius:4px;
                         font-size:12px;font-weight:bold;">{action_type}</span>
            {subtype_badge}
            <span style="color:#666;font-size:13px;margin-left:6px;">{action_ko}</span>
            <span style="margin-left:auto;color:#999;font-size:12px;">최근 7일 {repeat_label}</span>
          </div>

          {(
              '<div style="margin-bottom:14px;text-align:center;">'
              f'<img src="{a["creative_image_url"]}" width="480" '
              'style="max-width:100%;border-radius:6px;border:1px solid #e0e0e0;" '
              'alt="소재 이미지" />'
              '</div>'
          ) if a.get("creative_image_url") else ""}

          <table style="width:100%;font-size:13px;border-collapse:collapse;margin-bottom:12px;">
            <tr>
              <td style="padding:4px 8px;color:#888;width:110px;">Campaign</td>
              <td style="padding:4px 8px;font-family:monospace;font-size:12px;">{a['campaign_name']}</td>
            </tr>
            <tr style="background:#f9f9f9;">
              <td style="padding:4px 8px;color:#888;">Ad Set</td>
              <td style="padding:4px 8px;font-family:monospace;font-size:12px;">{a['adset_name']}</td>
            </tr>
            <tr>
              <td style="padding:4px 8px;color:#888;">Creative</td>
              <td style="padding:4px 8px;font-family:monospace;font-size:12px;">{a['ad_name']}</td>
            </tr>
            <tr style="background:#f9f9f9;">
              <td style="padding:4px 8px;color:#888;">Ad ID</td>
              <td style="padding:4px 8px;color:#555;font-size:12px;">{a['ad_id']}</td>
            </tr>
          </table>

          <h4 style="margin:12px 0 8px;color:#333;font-size:13px;">최근 6시간 성과</h4>
          <table style="border-collapse:collapse;width:100%;font-size:13px;">
            <thead>
              <tr style="background:#f0f4ff;">
                <th style="padding:6px 10px;border:1px solid #ddd;text-align:left;">지표</th>
                <th style="padding:6px 10px;border:1px solid #ddd;text-align:right;">기준치</th>
                <th style="padding:6px 10px;border:1px solid #ddd;text-align:right;">현재값</th>
                <th style="padding:6px 10px;border:1px solid #ddd;text-align:right;">기준 대비</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td style="padding:6px 10px;border:1px solid #ddd;">Spend_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">{spend_base:,.0f}원</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{a['spend_6h']:,.0f}원</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#27ae60;font-weight:bold;">+{(a['spend_6h']/spend_base-1)*100:.0f}%</td>
              </tr>
              <tr style="background:#f9f9f9;">
                <td style="padding:6px 10px;border:1px solid #ddd;">Clicks_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{clicks_6h:,}회</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
              </tr>
              <tr>
                <td style="padding:6px 10px;border:1px solid #ddd;">Purchases_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">{purch_base}건</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{int(a['purchases_6h'])}건</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#27ae60;font-weight:bold;">+{purch_diff}건</td>
              </tr>
              <tr style="background:#f9f9f9;">
                <td style="padding:6px 10px;border:1px solid #ddd;">Revenue_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{a['revenue_6h']:,.0f}원</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
              </tr>
              <tr>
                <td style="padding:6px 10px;border:1px solid #ddd;">ROAS_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">{roas_base:.0%}</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:{color};font-weight:bold;">{a['roas_6h']:.1%}</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#27ae60;font-weight:bold;">+{roas_diff_pp:.0f}%p</td>
              </tr>
              <tr style="background:#f9f9f9;">
                <td style="padding:6px 10px;border:1px solid #ddd;">ROAS_12h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{a['roas_12h']:.1%}</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
              </tr>
              <tr>
                <td style="padding:6px 10px;border:1px solid #ddd;">CTR_6h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">CTR_12h 기준</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{ctr_6h:.2%}</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:{'#27ae60' if ctr_diff_pp >= 0 else '#e74c3c'};font-weight:bold;">{'+' if ctr_diff_pp >= 0 else ''}{ctr_diff_pp:.1f}%p</td>
              </tr>
              <tr style="background:#f9f9f9;">
                <td style="padding:6px 10px;border:1px solid #ddd;">CTR_12h</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;">{ctr_12h:.2%}</td>
                <td style="padding:6px 10px;border:1px solid #ddd;text-align:right;color:#999;">-</td>
              </tr>
            </tbody>
          </table>

          <div style="margin-top:12px;padding:12px;background:#f8f9fa;border:1px solid #dee2e6;border-radius:6px;">
            <p style="margin:0 0 8px;font-size:12px;font-weight:bold;color:#495057;">기준 대비 상승폭</p>
            <p style="margin:0 0 4px;font-size:13px;color:#333;">
              &#8226; ROAS: 기준 {roas_base:.0%} 대비
              <strong style="color:{color};">+{roas_diff_pp:.0f}%p</strong>
              ({a['roas_6h']:.1%} vs {roas_base:.0%})
            </p>
            <p style="margin:0 0 4px;font-size:13px;color:#333;">
              &#8226; 구매건수: 기준 {purch_base}건 대비
              <strong style="color:{color};">+{purch_diff}건</strong>
              ({int(a['purchases_6h'])}건 vs {purch_base}건)
            </p>
            <p style="margin:0;font-size:13px;color:#333;">
              &#8226; CTR: 12시간 대비
              <strong style="color:{'#27ae60' if ctr_diff_pp >= 0 else '#e74c3c'};">{'+' if ctr_diff_pp >= 0 else ''}{ctr_diff_pp:.1f}%p</strong>
              (CTR_6h: {ctr_6h:.2%} vs CTR_12h: {ctr_12h:.2%})
            </p>
          </div>

          <div style="margin-top:10px;padding:12px;background:#f0f7ff;border-left:4px solid {color};border-radius:4px;">
            <p style="margin:0 0 4px;font-size:11px;color:#888;font-weight:bold;">AI 인사이트</p>
            <p style="margin:0;font-size:13px;color:#333;">{a['ai_insight']}</p>
          </div>
          <div style="margin-top:8px;padding:12px;background:#fff8e1;border-left:4px solid #f9a825;border-radius:4px;">
            <p style="margin:0 0 4px;font-size:11px;color:#888;font-weight:bold;">액션 가이드</p>
            <p style="margin:0;font-size:13px;color:#333;">{a['action_guide']}</p>
          </div>
          {build_stock_html(a.get("stock_info")) if a.get("stock_product") else ""}
        </div>
        """

    return f"""
    <html><body style="font-family:Arial,sans-serif;color:#333;max-width:700px;margin:0 auto;padding:20px;">
      <h2 style="color:#1a73e8;margin-bottom:4px;">Meta Ads Opportunity Alert</h2>
      <p style="color:#888;font-size:13px;margin-top:0;">
        {brand} &nbsp;|&nbsp; {now_kst.strftime('%Y-%m-%d %H:%M')} KST
      </p>
      {blocks}
      <p style="margin-top:24px;font-size:11px;color:#aaa;border-top:1px solid #eee;padding-top:12px;">
        * 동일 광고(ad_id)는 12시간 내 중복 발송되지 않습니다.
      </p>
    </body></html>
    """


def send_alert_email(alerts: list, cfg: dict) -> None:
    if not SMTP_USER or not SMTP_PASSWORD:
        print("[경고] SMTP 설정 없음 - 이메일 발송 건너뜀")
        return

    recipients = [r.strip() for r in cfg["alert_recipients"].split(",") if r.strip()]
    if not recipients:
        print(f"[경고] {cfg['brand']} 수신자 없음 - 이메일 발송 건너뜀")
        return

    types_str = ", ".join(sorted({a["action_type"] for a in alerts}))
    subject   = f"[Opportunity Alert - {types_str}] {cfg['brand']} ({len(alerts)}개 광고)"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(build_email_html(alerts, cfg["brand"]), "html", "utf-8"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_USER, recipients, msg.as_string())
        print(f"[완료] 이메일 발송 성공 ({cfg['brand']}) -> {', '.join(recipients)}")
    except Exception as e:
        print(f"[오류] 이메일 발송 실패 ({cfg['brand']}): {e}")


# ─────────────────────────────────────────
# Slack 알림
# ─────────────────────────────────────────
def send_slack_alert(alerts: list, cfg: dict) -> None:
    for a in alerts:
        alert_subtype = a.get("alert_subtype", "DEFAULT")
        is_br         = a.get("alert_type") == "BR"
        repeat_label  = f"{a['repeat_count']}회" if a["repeat_count"] > 1 else "첫 발생"
        ctr_6h        = a.get("ctr_6h", 0)
        ctr_12h       = a.get("ctr_12h", 0)
        ctr_diff_pp   = (ctr_6h - ctr_12h) * 100

        if is_br:
            # ── BR 브랜딩 슬랙 블록 ──
            br_color      = "#8e44ad" if alert_subtype == "BR_CTR_SURGE" else "#e74c3c"
            subtype_label = "CTR 상승형" if alert_subtype == "BR_CTR_SURGE" else "CTR 하락형"
            header_text   = f":bar_chart: *BR 브랜딩 Alert* — `{subtype_label}`"
            blocks = [
                {"type": "header", "text": {"type": "plain_text", "text": f"Meta Ads BR 브랜딩 Alert · {cfg['brand']}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": header_text}},
                {"type": "section", "fields": [
                    {"type": "mrkdwn", "text": f"*분류*\nBR 브랜딩  ·  최근 7일 {repeat_label}"},
                    {"type": "mrkdwn", "text": f"*채널*\n{a['channel']}"},
                    {"type": "mrkdwn", "text": f"*Campaign*\n`{a['campaign_name']}`"},
                    {"type": "mrkdwn", "text": f"*Ad Set*\n`{a['adset_name']}`"},
                    {"type": "mrkdwn", "text": f"*Creative*\n`{a['ad_name']}`"},
                    {"type": "mrkdwn", "text": f"*Ad ID*\n`{a['ad_id']}`"},
                ]},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": (
                    "*최근 6시간 브랜딩 지표*\n"
                    "```\n" +
                    ljust_dw("지표", 16) + rjust_dw("현재값", 12) + rjust_dw("12h 대비", 12) + "\n" +
                    "─" * 42 + "\n" +
                    ljust_dw("Impressions_6h", 16) + rjust_dw(f"{int(a.get('impressions_6h',0)):,}회", 12) + rjust_dw("─", 12) + "\n" +
                    ljust_dw("Clicks_6h", 16)      + rjust_dw(f"{int(a.get('clicks_6h',0)):,}회", 12)     + rjust_dw("─", 12) + "\n" +
                    ljust_dw("CTR_6h", 16)         + rjust_dw(f"{ctr_6h:.2%}", 12)                        + rjust_dw(('+' if ctr_diff_pp>=0 else '')+f"{ctr_diff_pp:.1f}%p", 12) + "\n" +
                    ljust_dw("CTR_12h", 16)        + rjust_dw(f"{ctr_12h:.2%}", 12)                       + rjust_dw("─", 12) + "\n" +
                    "```"
                )}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f":bulb: *AI 인사이트*\n{a.get('ai_insight', '')}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f":dart: *액션 가이드*\n{a.get('action_guide', '')}"}},
            ]
        else:
            # ── Performance 슬랙 블록 ──
            action_type   = a["action_type"]
            color         = ACTION_TYPE_COLOR.get(action_type, "#1a73e8")
            action_ko     = ACTION_TYPE_KO.get(action_type, action_type)
            ac            = _get_action_cond(cfg["brand"], action_type)
            opp           = _get_opp_filter(cfg["brand"])
            roas_base     = ac["roas_6h_min"]
            purch_base    = ac.get("purchases_6h_min", opp["purchases_6h_min"])
            spend_base    = ac.get("spend_6h_min",     opp["spend_6h_min"])
            roas_diff_pp  = (a["roas_6h"] - roas_base) * 100
            purch_diff    = int(a["purchases_6h"]) - purch_base
            subtype_label = {
                "CLICK_SURGE":           "클릭 급증형",
                "CONVERSION_EARLY":      "초기 전환 감지형",
                "CLICK_TO_CONVERT_GAP":  "전환 미흡형",
                "CONVERSION_SURGE":      "전환 급증형",
                "CONVERSION_SURGE_COLD": "첫 전환 급등형",
            }.get(alert_subtype, "")
            header_text = f":mega: *Opportunity Alert* — {action_type}" + (f"  `{subtype_label}`" if subtype_label else "")
            blocks = [
                {"type": "header", "text": {"type": "plain_text", "text": f"Meta Ads Opportunity Alert · {cfg['brand']}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": header_text}},
                {"type": "section", "fields": [
                    {"type": "mrkdwn", "text": f"*분류*\n{action_ko}  ·  최근 7일 {repeat_label}"},
                    {"type": "mrkdwn", "text": f"*채널*\n{a['channel']}"},
                    {"type": "mrkdwn", "text": f"*Campaign*\n`{a['campaign_name']}`"},
                    {"type": "mrkdwn", "text": f"*Ad Set*\n`{a['adset_name']}`"},
                    {"type": "mrkdwn", "text": f"*Creative*\n`{a['ad_name']}`"},
                    {"type": "mrkdwn", "text": f"*Ad ID*\n`{a['ad_id']}`"},
                ]},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": (
                    "*최근 6시간 성과*\n"
                    "```\n" +
                    ljust_dw("지표", 16) + rjust_dw("기준", 11) + rjust_dw("현재값", 13) + rjust_dw("대비", 11) + "\n" +
                    "─" * 53 + "\n" +
                    ljust_dw("Spend_6h", 16)    + rjust_dw(f"{spend_base:,.0f}원", 11) + rjust_dw(f"{a['spend_6h']:,.0f}원", 13)    + rjust_dw("─", 11) + "\n" +
                    ljust_dw("Clicks_6h", 16)   + rjust_dw("─", 11)         + rjust_dw(f"{int(a.get('clicks_6h',0)):,}회", 13)    + rjust_dw("─", 11) + "\n" +
                    ljust_dw("Purchases_6h", 16)+ rjust_dw(f"{purch_base}건", 11) + rjust_dw(f"{int(a['purchases_6h'])}건", 13)   + rjust_dw(('+' if purch_diff>=0 else '')+f"{purch_diff}건", 11) + "\n" +
                    ljust_dw("Revenue_6h", 16)  + rjust_dw("─", 11)         + rjust_dw(f"{a['revenue_6h']:,.0f}원", 13)           + rjust_dw("─", 11) + "\n" +
                    ljust_dw("ROAS_6h", 16)     + rjust_dw(f"{roas_base:.0%}", 11) + rjust_dw(f"{a['roas_6h']:.1%}", 13)         + rjust_dw(('+' if roas_diff_pp>=0 else '')+f"{roas_diff_pp:.0f}%p", 11) + "\n" +
                    ljust_dw("ROAS_12h", 16)    + rjust_dw("─", 11)         + rjust_dw(f"{a['roas_12h']:.1%}", 13)               + rjust_dw("─", 11) + "\n" +
                    ljust_dw("CTR_6h", 16)      + rjust_dw("12h기준", 11)   + rjust_dw(f"{ctr_6h:.2%}", 13)                      + rjust_dw(('+' if ctr_diff_pp>=0 else '')+f"{ctr_diff_pp:.1f}%p", 11) + "\n" +
                    ljust_dw("CTR_12h", 16)     + rjust_dw("─", 11)         + rjust_dw(f"{ctr_12h:.2%}", 13)                     + rjust_dw("─", 11) + "\n" +
                    "```"
                )}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": (
                    "*기준 대비 상승폭*\n"
                    f"• ROAS: 기준 {roas_base:.0%} 대비 *{'+' if roas_diff_pp>=0 else ''}{roas_diff_pp:.0f}%p* ({a['roas_6h']:.1%} vs {roas_base:.0%})\n"
                    f"• 구매건수: 기준 {purch_base}건 대비 *{'+' if purch_diff>=0 else ''}{purch_diff}건* ({int(a['purchases_6h'])}건 vs {purch_base}건)\n"
                    f"• CTR: 12시간 대비 *{'+' if ctr_diff_pp>=0 else ''}{ctr_diff_pp:.1f}%p* (CTR_6h: {ctr_6h:.2%} vs CTR_12h: {ctr_12h:.2%})"
                )}},
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": f":bulb: *AI 인사이트*\n{a.get('ai_insight', '')}"}},
                {"type": "section", "text": {"type": "mrkdwn", "text": f":dart: *액션 가이드*\n{a.get('action_guide', '')}"}},
            ]

        # 재고 섹션 추가 (BR/Performance 공통, 상품코드 파싱된 경우만)
        if a.get("stock_product"):
            stock_text = (
                f":package: *재고 현황* (`{a['stock_product']}`)\n"
                f"{a.get('stock_summary', '재고 정보 없음')}"
            )
            md_guide = a.get("stock_md_guide", "")
            if md_guide:
                stock_text += f"\n\n*MD 액션 가이드*\n{md_guide}"
            blocks.append({"type": "divider"})
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": stock_text}})

        # 소재 이미지가 있으면 상단에 추가
        if a.get("creative_image_url"):
            blocks.insert(2, {
                "type": "image",
                "image_url": a["creative_image_url"],
                "alt_text": a["ad_name"],
            })

        webhook_url = cfg["slack_webhook"]
        if not webhook_url:
            print(f"[경고] {cfg['brand']} SLACK_WEBHOOK_URL 없음 - 슬랙 발송 건너뜀 ({a['ad_name']})")
            continue
        try:
            resp = requests.post(
                webhook_url,
                json={"blocks": blocks},
                timeout=10,
            )
            if resp.status_code == 200:
                print(f"[완료] 슬랙 발송 성공 -> {a['ad_name']}")
            else:
                print(f"[오류] 슬랙 발송 실패 (HTTP {resp.status_code}): {resp.text}")
        except Exception as e:
            print(f"[오류] 슬랙 발송 실패: {e}")


# ─────────────────────────────────────────
# Meta API 호출
# ─────────────────────────────────────────
def _resolve_image_hash(image_hash: str) -> str:
    """image_hash → /adimages 원본 CDN URL. 실패 시 빈 문자열.

    Graph API 의 `hashes` 파라미터는 JSON 배열 문자열로 전달해야 함.
    """
    try:
        r = requests.get(
            f"https://graph.facebook.com/{API_VERSION}/{AD_ACCOUNT_ID}/adimages",
            params={"access_token": ACCESS_TOKEN,
                    "hashes": json.dumps([image_hash]),
                    "fields": "url,permalink_url"},
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json().get("data", [])
            if data:
                return data[0].get("url") or data[0].get("permalink_url") or ""
    except Exception:
        pass
    return ""


def fetch_creative_image(ad_id: str) -> str:
    """
    ad_id 기준으로 소재 이미지 URL 반환.
    우선순위:
      1) image_url
      2) object_story_id.full_picture (파트너십/인플루언서 IG 포스트 원본)
      3) image_hash → adimages.url (단일 이미지 광고)
      4) object_story_spec.video_data.image_hash → adimages.url (영상 광고 썸네일)
         또는 .link_data.image_hash (단일 이미지 광고의 다른 케이스)
      5) asset_feed_spec.images[].hash → adimages.url (ASC Dynamic 이미지)
         또는 asset_feed_spec.videos[].thumbnail_hash (ASC Dynamic 영상)
      6) object_story_spec.video_data.image_url (영상 광고 raw URL)
      7) thumbnail_url (최후, p64x64 = 64px 강제 리사이즈 → 슬랙에서 작게 표시)
    """
    try:
        resp = requests.get(
            f"https://graph.facebook.com/{API_VERSION}/{ad_id}",
            params={
                "access_token": ACCESS_TOKEN,
                "fields": "creative{thumbnail_url,image_url,image_hash,object_story_id,object_story_spec,asset_feed_spec}",
            },
            timeout=10,
        )
        if resp.status_code != 200:
            return ""
        creative = resp.json().get("creative", {})

        # 1) image_url이 원본 고화질이므로 최우선
        if creative.get("image_url"):
            return creative["image_url"]

        # 2) 파트너십(인플루언서) 소재: object_story_id로 인스타그램 포스팅 원본 이미지 조회
        object_story_id = creative.get("object_story_id")
        if object_story_id:
            post_resp = requests.get(
                f"https://graph.facebook.com/{API_VERSION}/{object_story_id}",
                params={
                    "access_token": ACCESS_TOKEN,
                    "fields": "full_picture",
                },
                timeout=10,
            )
            if post_resp.status_code == 200:
                full_picture = post_resp.json().get("full_picture")
                if full_picture:
                    return full_picture

        # 3) image_hash → adimages.url (단일 이미지 광고)
        image_hash = creative.get("image_hash")
        if image_hash:
            url = _resolve_image_hash(image_hash)
            if url:
                return url

        # 4) object_story_spec.{video_data|link_data}.image_hash
        oss = creative.get("object_story_spec") or {}
        for key in ("video_data", "link_data"):
            data = oss.get(key) or {}
            h = data.get("image_hash")
            if h:
                url = _resolve_image_hash(h)
                if url:
                    return url

        # 5) asset_feed_spec.images[].hash / .videos[].thumbnail_hash (ASC Dynamic)
        afs = creative.get("asset_feed_spec") or {}
        for img in (afs.get("images") or []):
            h = img.get("hash")
            if h:
                url = _resolve_image_hash(h)
                if url:
                    return url
                break
        for vid in (afs.get("videos") or []):
            h = vid.get("thumbnail_hash")
            if h:
                url = _resolve_image_hash(h)
                if url:
                    return url
                break

        # 6) video_data.image_url (영상 광고가 직접 노출한 썸네일 URL)
        vd_image_url = (oss.get("video_data") or {}).get("image_url")
        if vd_image_url:
            return vd_image_url

        # 7) 최후 fallback: thumbnail_url (저해상도, p64x64 = 64px)
        if creative.get("thumbnail_url"):
            return creative["thumbnail_url"]

        return ""
    except Exception as e:
        print(f"[경고] creative 이미지 조회 실패 (ad_id: {ad_id}): {e}")
        return ""


def fetch_insights() -> list:
    url    = f"https://graph.facebook.com/{API_VERSION}/{AD_ACCOUNT_ID}/insights"
    params = {
        "access_token": ACCESS_TOKEN,
        "level":        "ad",
        "date_preset":  "today",
        "fields": ",".join([
            "campaign_id", "campaign_name",
            "adset_id", "adset_name",
            "ad_id", "ad_name",
            "impressions", "clicks", "spend",
            "actions", "action_values",
        ]),
        "limit": 500,
    }

    all_data = []
    print(f"[정보] Meta API 호출 중... (버전: {API_VERSION})")

    while url:
        response = requests.get(url, params=params)
        if response.status_code != 200:
            err = response.json().get("error", {})
            print(f"[오류] API 호출 실패 (HTTP {response.status_code}): {err.get('message')}")
            return []
        body = response.json()
        all_data.extend(body.get("data", []))
        url    = body.get("paging", {}).get("next")
        params = {}

    return all_data


# ─────────────────────────────────────────
# 데이터 가공 -> DataFrame
# ─────────────────────────────────────────
def build_dataframe(raw_data: list) -> pd.DataFrame:
    snapshot_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    for item in raw_data:
        campaign_name = item.get("campaign_name", "")
        adset_name    = item.get("adset_name", "")

        rows.append({
            "SNAPSHOT_TS":     snapshot_ts,
            "CHANNEL":         detect_channel(campaign_name, adset_name),
            "AD_ACCOUNT_ID":   AD_ACCOUNT_ID,
            "CAMPAIGN_ID":     item.get("campaign_id"),
            "CAMPAIGN_NAME":   campaign_name,
            "ADSET_ID":        item.get("adset_id"),
            "ADSET_NAME":      adset_name,
            "AD_ID":           item.get("ad_id"),
            "AD_NAME":         item.get("ad_name"),
            "IMPRESSIONS_CUM": int(item.get("impressions", 0)),
            "CLICKS_CUM":      int(item.get("clicks", 0)),
            "SPEND_CUM":       float(item.get("spend", 0.0)),
            "PURCHASES_CUM":   extract_purchase_count(item.get("actions", [])),
            "REVENUE_CUM":     extract_purchase_revenue(item.get("action_values", [])),
        })

    return pd.DataFrame(rows)


# ─────────────────────────────────────────
# Snowflake
# ─────────────────────────────────────────
def _get_private_key_bytes() -> bytes:
    if SNOWFLAKE_PRIVATE_KEY_PATH:
        with open(SNOWFLAKE_PRIVATE_KEY_PATH, "rb") as f:
            pem = f.read()
    elif SNOWFLAKE_PRIVATE_KEY:
        pem = SNOWFLAKE_PRIVATE_KEY.replace("\\n", "\n").encode()
    else:
        raise ValueError("SNOWFLAKE_PRIVATE_KEY 또는 SNOWFLAKE_PRIVATE_KEY_PATH 중 하나를 설정하세요.")
    key = load_pem_private_key(pem, password=None, backend=default_backend())
    return key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )


def get_snowflake_conn():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        private_key=_get_private_key_bytes(),
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
    )


def load_to_snowflake(df: pd.DataFrame, cfg: dict) -> None:
    required = [SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, (SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PRIVATE_KEY_PATH),
                SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA]
    if not all(required):
        print("[경고] Snowflake 환경변수 누락 - 적재 건너뜀")
        return

    insert_cols = [
        "SNAPSHOT_TS", "BRAND", "AD_ACCOUNT_ID",
        "CAMPAIGN_ID", "CAMPAIGN_NAME",
        "ADSET_ID", "ADSET_NAME",
        "AD_ID", "AD_NAME",
        "IMPRESSIONS_CUM", "CLICKS_CUM", "SPEND_CUM", "PURCHASES_CUM", "REVENUE_CUM",
        "CHANNEL",
    ]
    df_insert = df[insert_cols]

    print(f"[정보] Snowflake 연결 중... ({SNOWFLAKE_ACCOUNT})")
    conn = get_snowflake_conn()
    try:
        cursor       = conn.cursor()
        cols         = ", ".join(insert_cols)
        placeholders = ", ".join(["%s"] * len(insert_cols))
        sql          = f"INSERT INTO {SNOWFLAKE_TABLE.upper()} ({cols}) VALUES ({placeholders})"
        rows         = [tuple(row) for row in df_insert.itertuples(index=False)]
        cursor.executemany(sql, rows)
        print(f"[완료] Snowflake 적재 성공 - {len(rows)}행")
    finally:
        conn.close()


# ─────────────────────────────────────────
# Alert 판단
# ─────────────────────────────────────────
def evaluate_alerts(df_now: pd.DataFrame, cfg: dict) -> None:
    required = [SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, (SNOWFLAKE_PRIVATE_KEY or SNOWFLAKE_PRIVATE_KEY_PATH),
                SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA]
    if not all(required):
        return

    # ── Snowflake에서 6h / 12h 전 스냅샷 조회 ──
    conn = get_snowflake_conn()
    try:
        cursor = conn.cursor()
        for hours, label in [(6, "6H"), (12, "12H")]:
            query = f"""
                SELECT ad_id, channel,
                       spend_cum, purchases_cum, revenue_cum,
                       clicks_cum, impressions_cum
                FROM (
                    SELECT ad_id, channel,
                           spend_cum, purchases_cum, revenue_cum,
                           clicks_cum, impressions_cum,
                           ROW_NUMBER() OVER (
                               PARTITION BY ad_id
                               ORDER BY ABS(DATEDIFF('minute',
                                   snapshot_ts,
                                   DATEADD('hour', -{hours},
                                       CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))
                               ))
                           ) AS rn
                    FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE.upper()}
                    WHERE brand = '{cfg["brand"]}'
                      AND snapshot_ts >= DATEADD('hour', -{hours + 2},
                              CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))
                      AND snapshot_ts <= DATEADD('hour', -{hours - 2},
                              CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()))
                ) WHERE rn = 1
            """
            cursor.execute(query)
            df_past = pd.DataFrame(
                cursor.fetchall(),
                columns=["AD_ID", "CHANNEL",
                         f"SPEND_{label}_PAST", f"PURCHASES_{label}_PAST", f"REVENUE_{label}_PAST",
                         f"CLICKS_{label}_PAST", f"IMPRESSIONS_{label}_PAST"],
            )
            df_now = df_now.merge(df_past, on=["AD_ID", "CHANNEL"], how="left")
    finally:
        conn.close()

    # ── 델타 / ROAS / CTR 계산 ──
    for label in ["6H", "12H"]:
        lbl = label.lower()
        ps  = pd.to_numeric(df_now[f"SPEND_{label}_PAST"],       errors="coerce").fillna(0.0)
        pp  = pd.to_numeric(df_now[f"PURCHASES_{label}_PAST"],   errors="coerce").fillna(0.0)
        pr  = pd.to_numeric(df_now[f"REVENUE_{label}_PAST"],     errors="coerce").fillna(0.0)
        pc  = pd.to_numeric(df_now[f"CLICKS_{label}_PAST"],      errors="coerce").fillna(0.0)
        pi  = pd.to_numeric(df_now[f"IMPRESSIONS_{label}_PAST"], errors="coerce").fillna(0.0)

        df_now[f"spend_{lbl}"]       = df_now["SPEND_CUM"]       - ps
        df_now[f"purchases_{lbl}"]   = df_now["PURCHASES_CUM"]   - pp
        df_now[f"revenue_{lbl}"]     = df_now["REVENUE_CUM"]     - pr
        df_now[f"clicks_{lbl}"]      = df_now["CLICKS_CUM"]      - pc
        df_now[f"impressions_{lbl}"] = df_now["IMPRESSIONS_CUM"] - pi

        df_now[f"roas_{lbl}"] = df_now.apply(
            lambda r, l=lbl: r[f"revenue_{l}"] / r[f"spend_{l}"] if r[f"spend_{l}"] > 0 else 0,
            axis=1,
        )
        df_now[f"ctr_{lbl}"] = df_now.apply(
            lambda r, l=lbl: r[f"clicks_{l}"] / r[f"impressions_{l}"] if r[f"impressions_{l}"] > 0 else 0,
            axis=1,
        )

    # ── prev_6h (직전 6시간 = 12h델타 - 6h델타) 계산 ──
    df_now["purchases_prev_6h"] = (df_now["purchases_12h"] - df_now["purchases_6h"]).clip(lower=0)
    df_now["clicks_prev_6h"]    = (df_now["clicks_12h"]    - df_now["clicks_6h"]).clip(lower=0)
    df_now["spend_prev_6h"]     = (df_now["spend_12h"]     - df_now["spend_6h"]).clip(lower=0)
    df_now["revenue_prev_6h"]   = (df_now["revenue_12h"]   - df_now["revenue_6h"]).clip(lower=0)
    df_now["roas_prev_6h"] = df_now.apply(
        lambda r: r["revenue_prev_6h"] / r["spend_prev_6h"] if r["spend_prev_6h"] > 0 else 0,
        axis=1,
    )

    # ── Alert 판단 ──
    print("\n" + "=" * 65)
    print("  ALERT 리포트")
    print("=" * 65)

    opp_alerts = []
    br_alerts  = []
    kill_found = False

    for _, row in df_now.iterrows():
        ad_info          = f"[{row.get('CHANNEL','OFFICIAL')}] {row['AD_NAME']} (ad_id: {row['AD_ID']})"

        # 카탈로그 광고는 동적으로 상품이 매핑돼 단일 소재 확인 불가 → 알럿 제외
        ad_name_lower = str(row.get("AD_NAME", "")).lower()
        if "com" in ad_name_lower and "카탈로그" in ad_name_lower:
            print(f"  [제외] 카탈로그 광고: {row['AD_NAME']}")
            continue

        campaign_tokens  = re.split(r'[\s_\-|/]+', str(row.get("CAMPAIGN_NAME", "")))
        is_br_campaign   = "BR" in [t.upper() for t in campaign_tokens]

        # ════════════════════════════════════
        # BR 브랜딩 캠페인 분기
        # ════════════════════════════════════
        if is_br_campaign:
            bc = _get_br_cond(cfg["brand"])
            br_gate = (
                row["impressions_6h"] >= bc["impressions_6h_min"]
                and row["clicks_6h"]  >= bc["clicks_6h_min"]
            )
            if not br_gate:
                continue

            br_subtype = determine_br_subtype(row["ctr_6h"], row["ctr_12h"], brand=cfg["brand"])
            if br_subtype is None:
                continue

            print(f"[BR/{br_subtype}] {ad_info}")
            print(f"  impressions_6h={int(row['impressions_6h']):,}  clicks_6h={int(row['clicks_6h']):,}"
                  f"  ctr_6h={row['ctr_6h']:.2%}  ctr_12h={row['ctr_12h']:.2%}")

            if not is_recently_alerted(row["AD_ID"], cfg["brand"]):
                repeat_count       = get_repeat_count(row["AD_ID"], cfg["brand"])
                creative_image_url = fetch_creative_image(row["AD_ID"])
                stock_info, stock_summary, stock_md_guide, stock_product = \
                    resolve_stock_for_ad(row["AD_NAME"], cfg)
                br_alert_data = {
                    "alert_type":         "BR",
                    "action_type":        "BR",
                    "alert_subtype":      br_subtype,
                    "channel":            row.get("CHANNEL", "OFFICIAL"),
                    "campaign_name":      row["CAMPAIGN_NAME"],
                    "adset_name":         row["ADSET_NAME"],
                    "ad_name":            row["AD_NAME"],
                    "ad_id":              row["AD_ID"],
                    "impressions_6h":     row["impressions_6h"],
                    "clicks_6h":          row["clicks_6h"],
                    "ctr_6h":             row["ctr_6h"],
                    "ctr_12h":            row["ctr_12h"],
                    "repeat_count":       repeat_count,
                    "creative_image_url": creative_image_url,
                    "stock_info":         stock_info,
                    "stock_summary":      stock_summary,
                    "stock_md_guide":     stock_md_guide,
                    "stock_product":      stock_product,
                }
                print(f"  -> Gemini 인사이트 생성 중...")
                insight, _ = generate_ai_insight(br_alert_data)
                br_alert_data["ai_insight"]   = insight
                br_alert_data["action_guide"] = build_action_guide(br_alert_data, stock_info)
                print(f"  AI: {insight}")
                print(f"  가이드: {br_alert_data['action_guide']}")
                br_alerts.append(br_alert_data)
            else:
                print(f"  -> 12시간 내 발송 이력 있음. 건너뜀.")
            continue

        # ════════════════════════════════════
        # Performance 캠페인 분기
        # ════════════════════════════════════

        # ── Opportunity Alert 공통 진입 조건 (브랜드별 임계치) ──
        roas_improving = row["roas_6h"] >= row["roas_12h"]
        opp_cfg = _get_opp_filter(cfg["brand"])
        opp_gate = (
            row["roas_6h"]      >= opp_cfg["roas_6h_min"]
            and row["spend_6h"] >= opp_cfg["spend_6h_min"]
            and row["purchases_6h"] >= opp_cfg["purchases_6h_min"]
            and roas_improving
        )

        if opp_gate:
            action_type = determine_action_type(
                row["roas_6h"], row["spend_6h"], row["purchases_6h"], brand=cfg["brand"]
            )
            if action_type is None:
                action_type = "CREATIVE_EXPANSION"

            _subtype_preview = determine_alert_subtype(
                row["ctr_6h"], row["ctr_12h"],
                row["purchases_6h"], row["roas_6h"], row["roas_12h"],
                row["purchases_prev_6h"], row["clicks_6h"],
                row["clicks_prev_6h"], row["roas_prev_6h"],
                row["purchases_12h"],
                brand=cfg["brand"],
            )
            print(f"[{action_type}/{_subtype_preview}] {ad_info}")
            print(f"  roas_6h={row['roas_6h']:.1%}  spend_6h={row['spend_6h']:,.0f}원"
                  f"  purchases_6h={int(row['purchases_6h'])}건  purchases_prev_6h={int(row['purchases_prev_6h'])}건"
                  f"  roas_prev_6h={row['roas_prev_6h']:.1%}"
                  f"  ctr_6h={row['ctr_6h']:.2%}  ctr_12h={row['ctr_12h']:.2%}")

            if not is_recently_alerted(row["AD_ID"], cfg["brand"]):
                repeat_count  = get_repeat_count(row["AD_ID"], cfg["brand"])
                alert_subtype = determine_alert_subtype(
                    row["ctr_6h"], row["ctr_12h"],
                    row["purchases_6h"], row["roas_6h"], row["roas_12h"],
                    row["purchases_prev_6h"], row["clicks_6h"],
                    row["clicks_prev_6h"], row["roas_prev_6h"],
                    brand=cfg["brand"],
                )
                print(f"  -> 소재 이미지 조회 중...")
                creative_image_url = fetch_creative_image(row["AD_ID"])
                if creative_image_url:
                    print(f"  -> 이미지 확보: {creative_image_url[:60]}...")
                else:
                    print(f"  -> 이미지 없음 (파트너십 광고 또는 영상 소재)")

                stock_info, stock_summary, stock_md_guide, stock_product = \
                    resolve_stock_for_ad(row["AD_NAME"], cfg)

                alert_data = {
                    "alert_type":         "PERFORMANCE",
                    "action_type":        action_type,
                    "alert_subtype":      alert_subtype,
                    "channel":            row.get("CHANNEL", "OFFICIAL"),
                    "campaign_name":      row["CAMPAIGN_NAME"],
                    "adset_name":         row["ADSET_NAME"],
                    "ad_name":            row["AD_NAME"],
                    "ad_id":              row["AD_ID"],
                    "roas_6h":            row["roas_6h"],
                    "roas_12h":           row["roas_12h"],
                    "roas_prev_6h":       row["roas_prev_6h"],
                    "spend_6h":           row["spend_6h"],
                    "purchases_6h":       row["purchases_6h"],
                    "purchases_prev_6h":  row["purchases_prev_6h"],
                    "revenue_6h":         row["revenue_6h"],
                    "clicks_6h":          row["clicks_6h"],
                    "clicks_prev_6h":     row["clicks_prev_6h"],
                    "ctr_6h":             row["ctr_6h"],
                    "ctr_12h":            row["ctr_12h"],
                    "repeat_count":       repeat_count,
                    "creative_image_url": creative_image_url,
                    "stock_info":         stock_info,
                    "stock_summary":      stock_summary,
                    "stock_md_guide":     stock_md_guide,
                    "stock_product":      stock_product,
                }
                print(f"  -> Gemini 인사이트 생성 중...")
                insight, _ = generate_ai_insight(alert_data)
                alert_data["ai_insight"]   = insight
                alert_data["action_guide"] = build_action_guide(alert_data, stock_info)
                print(f"  AI: {insight}")
                print(f"  가이드: {alert_data['action_guide']}")
                opp_alerts.append(alert_data)
            else:
                print(f"  -> 12시간 내 발송 이력 있음. 건너뜀.")

        # ── Kill Alert ──
        kl = KILL_CONDITION
        if row["roas_12h"] <= kl["roas_12h_max"] and row["spend_12h"] >= kl["spend_12h_min"]:
            print(f"[KILL] {ad_info}")
            print(f"  roas_12h={row['roas_12h']:.1%}  spend_12h={row['spend_12h']:,.0f}원")
            kill_found = True

    if not opp_alerts and not br_alerts and not kill_found:
        print("  현재 alert 조건에 해당하는 광고 없음")

    print("=" * 65)

    all_alerts = opp_alerts + br_alerts
    if all_alerts:
        send_alert_email(all_alerts, cfg)
        send_slack_alert(all_alerts, cfg)
        for a in all_alerts:
            mark_alert_sent(a["ad_id"], cfg["brand"])


# ─────────────────────────────────────────
# 메인 실행
# ─────────────────────────────────────────
if __name__ == "__main__":
    check_operating_hours()
    check_recent_snapshot_skip()
    raw_data = fetch_insights()
    if not raw_data:
        print("[결과] 데이터 없음. 오늘 활성 광고가 없거나 API 오류.")
        exit(0)

    df_full = build_dataframe(raw_data)
    print(f"\n[결과] 전체 행 수: {len(df_full)}")
    print("\n[결과] 상위 5개 행:")
    print(df_full[["CHANNEL", "AD_NAME", "SPEND_CUM", "PURCHASES_CUM", "REVENUE_CUM"]].head())

    for cfg in BRAND_CONFIGS:
        print(f"\n[{cfg['brand']}] 시작 - campaign_token={cfg['campaign_token']}, stock_brand_cd={cfg['stock_brand_cd']}")
        df_brand = filter_by_campaign_token(df_full, cfg["campaign_token"])
        df_brand = tag_brand(df_brand, cfg["brand"])
        print(f"[{cfg['brand']}] 필터 후 행 수: {len(df_brand)}")
        load_to_snowflake(df_brand, cfg)
        evaluate_alerts(df_brand, cfg)
