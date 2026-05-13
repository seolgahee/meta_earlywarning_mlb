"""실제 BR 광고 하나로 stock_summary / md_guide / action_guide 출력 예시."""
import os, sys
sys.stdout.reconfigure(encoding="utf-8")

# app.py 함수 직접 import
from dotenv import load_dotenv
load_dotenv()

# app.py 의 함수들 import하려면 그쪽이 import 시점에 ACCESS_TOKEN 검증함.
# 우회: import 전에 env 확인 후 import
import app

# 7FSDCB463 (BR 키즈 광고 — 5/12 알럿 검증된 케이스)
test_cases = [
    # (광고명, brand, alert_type, action_type, alert_subtype, performance fields)
    {
        "label": "BR 키즈 광고 (7FSDCB463) — CTR_SURGE 시뮬",
        "ad_name": "260422_SD_샌들_이미지_상품_상품화보_스플래시썸머-7FSDCB463",
        "campaign_name": "I_DA_N_MT_ALL_MAN_BR_OWN_COM",
        "brand": "MLB_KIDS",
        "alert_type": "BR",
        "action_type": "BR",
        "alert_subtype": "BR_CTR_SURGE",
        "perf": {
            "impressions_6h": 5_400, "clicks_6h": 230, "ctr_6h": 0.043, "ctr_12h": 0.039,
        },
    },
    {
        "label": "Performance 성인 — CAMPAIGN_SCALE 시뮬",
        "ad_name": "M_DA_Y_MT_ALL_ASC_CV_OWN_BNR-3ACPB245N",
        "campaign_name": "M_DA_Y_MT_ALL_ASC_CV_OWN_BNR",
        "brand": "MLB",
        "alert_type": "PERFORMANCE",
        "action_type": "CAMPAIGN_SCALE",
        "alert_subtype": "CONVERSION_SURGE",
        "perf": {
            "spend_6h": 35_000, "purchases_6h": 5, "revenue_6h": 250_000,
            "roas_6h": 7.14, "roas_12h": 4.8, "ctr_6h": 0.06, "ctr_12h": 0.048,
            "clicks_6h": 180, "purchases_prev_6h": 2,
        },
    },
]

# brand별 cfg 흉내
cfgs = {c["brand"]: c for c in app.BRAND_CONFIGS}

for tc in test_cases:
    print("\n" + "=" * 90)
    print(f"  {tc['label']}")
    print("=" * 90)
    print(f"AD_NAME: {tc['ad_name']}")
    print(f"CAMPAIGN: {tc['campaign_name']}")

    cfg = cfgs[tc["brand"]]

    # 1) 품번/컬러 추출
    codes = app.extract_product_code(tc["ad_name"])
    print(f"\n[1] 품번 파싱: {codes}")

    # 2) 재고 조회
    print(f"\n[2] resolve_stock_for_ad 실행:")
    stock_info, stock_summary, stock_md_guide, stock_product = \
        app.resolve_stock_for_ad(tc["ad_name"], cfg)
    print(f"\n  stock_product: {stock_product}")
    print(f"\n  ── stock_summary (재고 요약) ──")
    print("  " + (stock_summary or "(없음)").replace("\n", "\n  "))
    print(f"\n  ── stock_md_guide (MD 액션) ──")
    print("  " + (stock_md_guide or "(없음)").replace("\n", "\n  "))

    # 3) action_guide 생성
    alert_data = {
        "alert_type":    tc["alert_type"],
        "action_type":   tc["action_type"],
        "alert_subtype": tc["alert_subtype"],
        "ad_name":       tc["ad_name"],
        **tc["perf"],
    }
    action_guide = app.build_action_guide(alert_data, stock_info)
    print(f"\n[3] action_guide (광고 액션):")
    print("  " + (action_guide or "(없음)").replace("\n", "\n  "))
