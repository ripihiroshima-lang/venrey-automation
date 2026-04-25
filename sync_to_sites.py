#!/usr/bin/env python3
"""
ベンリー「出勤情報をサイトへ更新」(scheduleupdate) を自動化:
- 引数のスタッフ名（カンマ区切り）を左ペインで個別チェック
- 右ペインの全サイトを全選択
- 「出勤情報を更新」ボタンをクリック
- 確認ダイアログ対応
"""
import os, sys, time
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# 店舗
LOGIN_URL = "https://mrvenrey.jp/"
SYNC_URL = "https://mrvenrey.jp/#/menu/scheduleupdate"

STORES = {
    "fuwamoko": {"id": "rd67",       "password": "52a4et7"},
    "crea":     {"id": "GRP001121",  "password": "hj6bf3fwck"},
}


def login(page, store):
    page.goto(LOGIN_URL, timeout=30000)
    page.wait_for_load_state("networkidle", timeout=30000)
    page.locator("input").first.fill(store["id"])
    page.locator('input[type="password"]').fill(store["password"])
    page.locator('button[type="submit"], button:has-text("ログイン")').first.click()
    page.wait_for_load_state("networkidle", timeout=30000)
    page.wait_for_selector("text=週間スケジュール", state="visible", timeout=30000)
    time.sleep(2)


def sync_for_store(page, store_label, target_staff_names):
    """対象スタッフを個別チェックして全サイト配信。"""
    print(f"\n=== [{store_label}] 配信ページへ移動 ===")
    page.goto(SYNC_URL, timeout=30000)
    page.wait_for_load_state("networkidle", timeout=30000)
    page.wait_for_selector('label.checkbox-ex2', state='visible', timeout=20000)
    time.sleep(3)

    # 個別チェックボックスを名前で特定
    items = page.evaluate("""
        () => {
            const labels = [...document.querySelectorAll('label.checkbox-ex2[listallcheckmode="false"]')];
            return labels.map((l, idx) => {
                let p = l;
                for (let i = 0; i < 5; i++) {
                    if (p.parentElement) p = p.parentElement;
                    else break;
                }
                const text = (p.innerText || '').replace(/\\s+/g, ' ').trim();
                const rect = l.getBoundingClientRect();
                return {idx, x: Math.round(rect.x), text};
            });
        }
    """)
    individual_labels = page.locator('label.checkbox-ex2[listallcheckmode="false"]')

    # 対象スタッフを個別チェック（左ペイン x<500）。idx=0はヘッダーCBなのでスキップ。
    matched = []
    for it in items:
        if it['idx'] == 0:  # ヘッダーCB（編集順に並び替え）スキップ
            continue
        if it['x'] >= 500:  # 右ペイン（サイト）はスキップ
            continue
        tokens = it['text'].split()
        for name in target_staff_names:
            if name in tokens or it['text'].strip() == name:
                individual_labels.nth(it['idx']).click()
                matched.append(name)
                print(f"  ✓ スタッフチェック: {name} (idx={it['idx']})")
                time.sleep(0.4)
                break

    not_found = [n for n in target_staff_names if n not in matched]
    if not_found:
        print(f"  ⚠ 未発見スタッフ: {not_found}")

    if not matched:
        print(f"  対象スタッフが見つかりません。スキップ")
        return False

    # サイトはテキスト一致で識別（idx の不安定さを回避）
    SITE_NAMES = ["エステラブ", "エステ魂", "オフィシャル", "全国メンズエステランキング"]
    site_count = 0
    for it in items:
        if it['idx'] == 0:
            continue
        if it['x'] < 500:  # 左ペイン（スタッフ）はスキップ
            continue
        text = it['text']
        # サイト名のいずれかが含まれていればチェック
        matched_site = next((s for s in SITE_NAMES if s in text), None)
        if not matched_site:
            print(f"  - スキップ idx={it['idx']} (サイト名一致なし): {text[:40]}")
            continue
        individual_labels.nth(it['idx']).click()
        time.sleep(0.3)
        print(f"  ✓ サイト個別チェック: '{matched_site}' (idx={it['idx']})")
        site_count += 1
    print(f"  対象サイト {site_count} 件チェック完了")

    # 「出勤情報を更新」ボタン（右上の赤いボタン）
    update_btns = page.locator('button:has-text("出勤情報を更新")')
    btn = update_btns.last
    btn.scroll_into_view_if_needed(timeout=3000)
    btn.click()
    print(f"  ✓ 「出勤情報を更新」ボタンクリック")

    # ダイアログ表示まで少し待つ
    time.sleep(2)

    # ダイアログ検出: 表示中のあらゆるボタンの文字列を取得
    dialog_buttons = page.evaluate("""
        () => {
            const all = [...document.querySelectorAll('button, .button, [role="button"]')];
            return all.filter(b => {
                const r = b.getBoundingClientRect();
                return r.width > 0 && r.height > 0;
            }).map((b, i) => ({
                idx: i,
                text: (b.textContent || '').replace(/\\s+/g, ' ').trim().slice(0, 40),
                x: Math.round(b.getBoundingClientRect().x),
                y: Math.round(b.getBoundingClientRect().y),
            }));
        }
    """)
    print(f"  ボタン候補（クリック後）:")
    for db in dialog_buttons:
        if db['text']:
            print(f"    [{db['idx']:3d}] x={db['x']:4d} y={db['y']:4d}: {db['text']}")

    # 確認ダイアログの承認ボタン: テキスト末尾完全一致で判定（CSS文字列の誤爆を回避）
    confirm_clicked = False
    exact_targets = ["出勤情報を更新開始", "更新開始", "更新する", "実行する", "送信する",
                     "実行", "送信", "確定"]
    for db in dialog_buttons:
        txt = db['text'].strip() if db.get('text') else ""
        # 元の「出勤情報を更新」自身はスキップ（完全一致または末尾一致）
        if txt == "出勤情報を更新" or txt.endswith("出勤情報を更新"):
            continue
        if not any(txt == t or txt.endswith(t) for t in exact_targets):
            continue
        # 該当ボタンをクリック
        try:
            all_btns = page.locator("button, .button, [role='button']")
            target = all_btns.nth(db['idx'])
            if target.is_visible():
                target.click(timeout=3000)
                print(f"  ✓ ダイアログ承認: '{txt}'")
                confirm_clicked = True
                break
        except Exception as e:
            print(f"    クリック失敗: {e}")
            continue

    if not confirm_clicked:
        print(f"  ⚠ ダイアログ承認ボタン未検出 → ダイアログなしで処理が走った可能性")
        print(f"     → 中断: ボタンを押さずブラウザを閉じます（ベンリーへ影響なし）")
        return False

    # サーバー応答待ち（長めに）
    print("  サーバー処理を待機中（30秒）...")
    time.sleep(30)

    try:
        page.wait_for_load_state("networkidle", timeout=60000)
    except PlaywrightTimeout:
        pass
    time.sleep(5)
    print(f"  → [{store_label}] 配信送信完了")
    return True


def main():
    selected = os.environ.get("SELECTED_STAFF", "").strip()
    if not selected:
        print("ERROR: SELECTED_STAFF 環境変数が必要です")
        return 1
    target_staff = [s.strip() for s in selected.split(",") if s.strip()]
    store_label = os.environ.get("STORE", "fuwamoko").strip()
    if store_label not in STORES:
        print(f"ERROR: STORE='{store_label}' は無効。{list(STORES.keys())} のいずれか")
        return 1

    print(f"対象店舗: {store_label}")
    print(f"対象スタッフ: {target_staff}")

    headless = os.environ.get("HEADLESS", "true").lower() not in ("0", "false", "no")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        context.set_default_timeout(30000)
        page = context.new_page()
        page.set_viewport_size({"width": 1600, "height": 900})

        login(page, STORES[store_label])
        sync_for_store(page, store_label, target_staff)

        time.sleep(3)
        browser.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
