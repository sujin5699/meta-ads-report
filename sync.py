"""
Meta Marketing API → data.json 동기화 스크립트
GitHub Actions에서 매일 자동 실행됨
"""

import os, json, requests
from datetime import datetime, timedelta, timezone

# ── 설정
TOKEN = os.environ.get("META_TOKEN", "")
DATE_FROM = os.environ.get("DATE_FROM", "").strip()
DATE_TO   = os.environ.get("DATE_TO",   "").strip()

KST = timezone(timedelta(hours=9))
yesterday = (datetime.now(KST) - timedelta(days=1)).strftime("%Y-%m-%d")
if not DATE_FROM: DATE_FROM = yesterday
if not DATE_TO:   DATE_TO   = yesterday

CHANNELS = {
    "브스":   "act_4070941403163951",
    "공식몰": "act_914056404233971",
    "컬리":   "act_725945530284711",
}

API = "https://graph.facebook.com/v19.0"

FIELDS = ",".join([
    "campaign_name","adset_name","ad_name","ad_id","date_start","objective",
    "spend","impressions","reach","frequency","clicks","cpc","cpm","ctr",
    "actions","action_values","cost_per_action_type","purchase_roas",
])

def fetch_insights(act_id, channel, date_from, date_to):
    url = f"{API}/{act_id}/insights"
    params = {
        "access_token": TOKEN,
        "level": "ad",
        "fields": FIELDS,
        "time_range": json.dumps({"since": date_from, "until": date_to}),
        "time_increment": 1,
        "limit": 500,
    }
    rows = []
    while url:
        r = requests.get(url, params=params, timeout=30)
        data = r.json()
        if "error" in data:
            print(f"  ⚠ {channel} 오류: {data['error']['message']}")
            break
        for item in data.get("data", []):
            rows.append(convert_row(item, channel))
        url    = data.get("paging", {}).get("next")
        params = {}
    return rows

def get_action(actions, action_type):
    if not actions: return ""
    for a in actions:
        if a["action_type"] == action_type:
            return a.get("value", "")
    return ""

def convert_row(item, channel):
    actions       = item.get("actions", [])
    action_values = item.get("action_values", [])
    cost_per      = item.get("cost_per_action_type", [])
    purchase     = get_action(actions, "omni_purchase") or get_action(actions, "purchase")
    purchase_val = get_action(action_values, "omni_purchase") or get_action(action_values, "purchase")
    cost_per_pur = get_action(cost_per, "omni_purchase") or get_action(cost_per, "purchase")
    objective    = item.get("objective", "")
    result_type_map = {
        "OUTCOME_SALES": "omni_purchase", "LINK_CLICKS": "link_click",
        "OUTCOME_AWARENESS": "post_impression", "OUTCOME_TRAFFIC": "link_click",
    }
    result_type = result_type_map.get(objective, "")
    result_val  = get_action(actions, result_type) if result_type else ""
    result_cost = get_action(cost_per, result_type) if result_type else ""
    roas_list   = item.get("purchase_roas", [])
    roas        = roas_list[0].get("value", "") if roas_list else ""
    return {
        "캠페인 이름": item.get("campaign_name",""), "광고 세트 이름": item.get("adset_name",""),
        "광고 이름": item.get("ad_name",""), "광고 ID": item.get("ad_id",""),
        "일": item.get("date_start",""), "목표": objective,
        "게재 상태": "active", "게재 수준": "ad", "기여 설정": "클릭 후 7일, 조회 후 1일",
        "시작": item.get("date_start",""), "종료": item.get("date_stop",""),
        "지출 금액 (KRW)": item.get("spend","0"), "노출": item.get("impressions","0"),
        "CPM(1,000회 노출당 비용)": item.get("cpm",""), "도달": item.get("reach","0"),
        "빈도": item.get("frequency",""), "링크 클릭": item.get("clicks","0"),
        "CPC(링크 클릭당 비용)": item.get("cpc",""), "CTR(전체)": item.get("ctr",""),
        "구매": purchase, "구매당 비용": cost_per_pur, "구매 전환값": purchase_val,
        "구매 ROAS(광고 지출 대비 수익률)": roas,
        "결과 유형": result_type, "결과": result_val, "결과당 비용": result_cost,
        "보고 시작": item.get("date_start",""), "보고 종료": item.get("date_stop",""),
        "__channel": channel,
    }

def fetch_thumbnails(ad_ids):
    """광고 ID 목록 → { ad_id: { type, thumbUrl } }"""
    result = {}
    batch = list(set(ad_ids))
    for i in range(0, len(batch), 25):
        chunk = batch[i:i+25]
        ids_str = ",".join(chunk)
        fields = "id,creative{thumbnail_url,object_story_spec,video_id}"
        url = f"{API}/?ids={ids_str}&fields={fields}&access_token={TOKEN}"
        try:
            r = requests.get(url, timeout=30)
            data = r.json()
            if "error" in data:
                print(f"  ⚠ 썸네일 오류: {data['error']['message']}")
                continue
            for ad_id, ad in data.items():
                creative = ad.get("creative", {})
                if not creative:
                    continue
                thumb_url  = ""
                thumb_type = "image"
                # 영상 소재
                vid_id = creative.get("video_id") or \
                    ((creative.get("object_story_spec") or {}).get("video_data") or {}).get("video_id")
                if vid_id:
                    thumb_type = "video"
                    vr    = requests.get(f"{API}/{vid_id}?fields=thumbnails&access_token={TOKEN}", timeout=20)
                    vdata = vr.json()
                    thumbs = (vdata.get("thumbnails") or {}).get("data") or []
                    thumb_url = thumbs[0].get("uri", "") if thumbs else ""
                if not thumb_url:
                    thumb_url = creative.get("thumbnail_url", "")
                if thumb_url:
                    result[ad_id] = {"type": thumb_type, "thumbUrl": thumb_url}
        except Exception as e:
            print(f"  ⚠ 썸네일 배치 실패: {e}")
    return result

def main():
    if not TOKEN:
        print("❌ META_TOKEN 환경변수가 없습니다.")
        return

    print(f"📅 수집 기간: {DATE_FROM} ~ {DATE_TO}")

    try:
        with open("data.json", "r", encoding="utf-8") as f:
            existing = json.load(f)
        existing_rows  = existing.get("raw", [])
        existing_files = existing.get("files", [])
        thumb_cache    = existing.get("thumbs", {})
        print(f"📂 기존 데이터: {len(existing_rows)}행, 썸네일 캐시: {len(thumb_cache)}개")
    except:
        existing_rows, existing_files, thumb_cache = [], [], {}
        print("📂 기존 데이터 없음, 새로 생성")

    new_rows = []
    for channel, act_id in CHANNELS.items():
        print(f"🔄 {channel} ({act_id}) 수집 중...")
        rows = fetch_insights(act_id, channel, DATE_FROM, DATE_TO)
        print(f"  → {len(rows)}행")
        new_rows.extend(rows)

    if not new_rows:
        print("⚠ 새 데이터 없음")
        return

    def row_key(r):
        return str(r.get("광고 ID","")) + "|" + str(r.get("일","")) + "|" + str(r.get("__channel",""))

    key_index = {row_key(r): i for i, r in enumerate(existing_rows)}
    added = overwritten = 0
    for r in new_rows:
        k = row_key(r)
        if k in key_index:
            existing_rows[key_index[k]] = r
            overwritten += 1
        else:
            key_index[k] = len(existing_rows)
            existing_rows.append(r)
            added += 1

    # 썸네일 — 캐시 없는 ID만 수집
    all_ad_ids = list({
        "".join(c for c in str(r.get("광고 ID","")).split("|")[0] if c.isdigit())
        for r in existing_rows if r.get("광고 ID")
    })
    uncached = [aid for aid in all_ad_ids if aid and aid not in thumb_cache]
    if uncached:
        print(f"🖼 썸네일 수집 중... ({len(uncached)}개 신규)")
        new_thumbs = fetch_thumbnails(uncached)
        thumb_cache.update(new_thumbs)
        print(f"  → {len(new_thumbs)}개 수집")
    else:
        print("🖼 썸네일 모두 캐시됨")

    existing_files.append({
        "name": f"API sync {DATE_FROM}~{DATE_TO}", "channel": "API",
        "count": len(new_rows), "added": added, "overwritten": overwritten,
    })

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump({
            "raw": existing_rows, "files": existing_files,
            "thumbs": thumb_cache, "updatedAt": datetime.now(KST).isoformat(),
        }, f, ensure_ascii=False)

    print(f"✅ 완료: +{added}행 추가, {overwritten}행 갱신, 썸네일 {len(thumb_cache)}개")

if __name__ == "__main__":
    main()
