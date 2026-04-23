#!/usr/bin/env python3
"""
Mr.Venrey 週間スケジュール自動更新 - テスト用シート版 (C-040)

C-012 (main.py) とは別系統。参照データをテスト用シートに切替えたバージョン。
既存 main.py の Playwright / ベンリー操作ロジックをそのまま再利用し、
シート読み込み部分だけ本ファイルで上書きする。

実行方法（手動トリガー専用）:
    python main_test.py              # 本番更新
    DRY_RUN=true python main_test.py # 読み込みのみ（ベンリー操作なし）
"""

import json
import os
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime, date

import pandas as pd

# ============================================================
# テスト用シート固有の設定
# ============================================================
# テスト用スプレッドシート（タイトル: "2026年4月 シフト表 テスト用"）
SPREADSHEET_ID = "1IydMT3vlET1hJBwpQJ1EZjx1wCrRsw2AxsV7Vxrn5dM"

# 店舗インデックス → タブ名（STORES[i] に対応）
STORE_SHEETS = ["CREA", "ふわもこ"]

# 集計行マーカー（先頭に現れたらスキップ）
AGGREGATE_MARKERS = ("出勤人数", "当日欠勤", "事前欠勤", "店欠", "店都合")


# ============================================================
# OAuth / Sheets 取得（main.py の実装を踏襲 / Playwrightに依存しない）
# ============================================================
def _get_access_token():
    client_id     = os.environ.get("GOOGLE_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET")
    refresh_token = os.environ.get("GOOGLE_REFRESH_TOKEN")
    if not all([client_id, client_secret, refresh_token]):
        oauth_path = os.path.expanduser("~/.config/gcp-oauth.keys.json")
        creds_path = os.path.expanduser("~/.config/gdrive-server-credentials.json")
        with open(oauth_path) as f:
            oauth = json.load(f)
        with open(creds_path) as f:
            creds = json.load(f)
        client_id     = oauth["installed"]["client_id"]
        client_secret = oauth["installed"]["client_secret"]
        refresh_token = creds["refresh_token"]
    data = urllib.parse.urlencode({
        "client_id":     client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type":    "refresh_token",
    }).encode()
    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token", data=data, method="POST"
    )
    with urllib.request.urlopen(req) as r:
        return json.load(r)["access_token"]


def _fetch_sheet_df(sheet_name):
    try:
        access_token = _get_access_token()
        encoded_range = urllib.parse.quote(sheet_name)
        url = (
            f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}"
            f"/values/{encoded_range}"
        )
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {access_token}"})
        with urllib.request.urlopen(req) as r:
            data = json.load(r)
        rows = data.get("values", [])
        if not rows:
            return None
        max_cols = max(len(row) for row in rows)
        padded = [row + [""] * (max_cols - len(row)) for row in rows]
        return pd.DataFrame(padded, dtype=str)
    except Exception as e:
        print(f"シート取得エラー ({sheet_name}): {e}")
        return None


def _normalize_name(raw):
    """スタッフ名を正規化（main.py と同等）。"""
    name = raw.strip().replace(" ", "").replace("　", "")
    name = re.sub(r'[（(][^）)]*[）)]', '', name)
    name = re.sub(r'\d+/\d+[A-Za-z]*$', '', name)
    for _ in range(3):
        name = re.sub(r'\d+$', '', name)
        name = re.sub(r'[A-Za-z]+$', '', name)
    return name


# ============================================================
# シフトセル解析（新シート表記対応）
# ============================================================
def parse_time_cell(cell_value):
    """
    テスト用シートのシフトセル値を (開始, 終了) にパースする。

    対応フォーマット:
      "13:00〜22:50上"     → ("13:00", "22:50")
      "18:00 〜 24:00 上"  → ("18:00", "24:00")
      "12-15:30上"         → ("12:00", "15:30")
      "19-23上"            → ("19:00", "23:00")
      "21:30-27上"         → ("21:30", "27:00")
      "19~3受"             → ("19:00", "27:00")  # 終了が開始より小 → +24h
      "12-27受"            → ("12:00", "27:00")
      "OFF" / "当欠" / "前欠" / "店欠" / "当欠店" → "休み"
    """
    if cell_value is None:
        return None
    s = str(cell_value).strip()
    if not s or s == "nan":
        return None

    # OFF / 欠勤系表記 → 休み
    if s.upper() == "OFF":
        return "休み"
    if re.match(r'^(当欠|前欠|店欠)', s):
        return "休み"

    # セパレータ・空白を統一
    normalized = s.replace("〜", "-").replace("~", "-")
    normalized = re.sub(r'\s+', '', normalized)

    # 時刻ペア抽出: HH(:MM)?-HH(:MM)?
    m = re.match(r'^(\d{1,2})(?::(\d{1,2}))?-(\d{1,2})(?::(\d{1,2}))?', normalized)
    if m:
        sh = int(m.group(1))
        sm = int(m.group(2)) if m.group(2) else 0
        eh = int(m.group(3))
        em = int(m.group(4)) if m.group(4) else 0

        # 終了が開始より小さい → 翌日またぎ（19-3 → 19:00-27:00）
        if eh < sh:
            eh += 24

        # 開始が 24 以上は 24 引く（Venrey は深夜帯 25:00 表記を許容）
        if sh >= 24:
            sh -= 24

        # 範囲チェック
        if not (0 <= sh < 24 and 0 <= sm < 60 and 0 <= em < 60):
            return None
        if eh > 30:
            return None

        return f"{sh:02d}:{sm:02d}", f"{eh:02d}:{em:02d}"

    # 数字を含まないセル（例: "確認中"）→ スキップ
    if not re.search(r"\d", s):
        return None

    # ハイフンなし数字のみ → 既存仕様を踏襲し「休み」扱い
    return "休み"


# ============================================================
# 日付マップ構築（新シート用: 日付ヘッダ行と開始列を明示）
# ============================================================
def _build_date_map(df, base_year, base_month, header_row_idx, date_start_col):
    """
    df のヘッダ行から {col_index: date} を作る。
    "5月→1" のような月切替ヒントを検出して翌月に繰り上げる。
    日付が前列より小さくなった場合も翌月に繰り上げる（保険）。
    """
    date_map = {}
    cur_year, cur_month = base_year, base_month
    prev_day = 0
    for col_idx in range(date_start_col, df.shape[1]):
        val = df.iloc[header_row_idx, col_idx]
        if val is None:
            continue
        s = str(val).strip()
        if not s:
            continue

        # 月切替ヒント ("5月→1", "5月\n1" など)
        month_hint = re.search(r'(\d+)月', s)
        day_match = None
        if month_hint:
            hinted_month = int(month_hint.group(1))
            if hinted_month != cur_month:
                cur_month = hinted_month
                # 12月→1月 なら年も繰り上げ
                if hinted_month == 1 and base_month == 12:
                    cur_year = base_year + 1
                prev_day = 0
            # 月表記の後ろから日を取る
            after = s.split("月", 1)[1]
            after = after.replace("→", "")
            day_match = re.search(r'(\d+)', after)
        else:
            day_match = re.match(r'^(\d+)', s)

        if not day_match:
            continue
        day = int(day_match.group(1))
        if not (1 <= day <= 31):
            continue
        # 月ヒント無しで日付リセット → 翌月繰り上げ
        if day < prev_day and not month_hint:
            if cur_month == 12:
                cur_year, cur_month = cur_year + 1, 1
            else:
                cur_month += 1
        try:
            date_map[col_idx] = date(cur_year, cur_month, day)
        except ValueError:
            pass
        prev_day = day
    return date_map


# ============================================================
# スタッフ行パース（1タブ=1店舗用・集計行スキップ）
# ============================================================
def _parse_staff_rows_for_store(df, date_map, data_start_row):
    schedule = {}
    for row_idx in range(data_start_row, df.shape[0]):
        if df.shape[1] == 0:
            continue
        raw_name = str(df.iloc[row_idx, 0]).strip()
        if not raw_name or raw_name == "nan":
            continue
        # 集計行スキップ
        if any(marker in raw_name for marker in AGGREGATE_MARKERS):
            continue
        # 漢字/かな/英数を含まない行（絵文字のみなど）はスキップ
        if not re.search(r'[\w぀-ヿ一-鿿]', raw_name):
            continue

        name = _normalize_name(raw_name)
        if not name:
            continue
        schedule.setdefault(name, {})
        for col_idx, d in date_map.items():
            if col_idx >= df.shape[1]:
                continue
            parsed = parse_time_cell(df.iloc[row_idx, col_idx])
            if parsed:
                schedule[name][d] = parsed
    return schedule


# ============================================================
# スケジュール読み込み（CREA / ふわもこ タブを個別に処理）
# ============================================================
def load_schedule():
    print("[C-040] テスト用スプレッドシートを読み込み中...")
    today = datetime.now()
    fallback_year, fallback_month = today.year, today.month

    schedules = [{}, {}]
    for i, sheet_name in enumerate(STORE_SHEETS):
        df = _fetch_sheet_df(sheet_name)
        if df is None:
            print(f"  [{sheet_name}] タブが取得できません")
            continue

        # タブ1行目のタイトルから年月を検出 (例: "2026年4月  シフト表　【CREA】")
        base_year, base_month = fallback_year, fallback_month
        if df.shape[0] > 0 and df.shape[1] > 0:
            title_cell = str(df.iloc[0, 0])
            m = re.search(r'(\d{4})年(\d{1,2})月', title_cell)
            if m:
                base_year = int(m.group(1))
                base_month = int(m.group(2))
                print(f"  [{sheet_name}] タイトルから {base_year}年{base_month}月 を検出")
            else:
                print(f"  [{sheet_name}] タイトル検出失敗 → 現在日付 {base_year}年{base_month}月 を採用")

        # 新シート構造: 0行目=タイトル / 1行目=日付ヘッダ / 2行目以降=スタッフ / 先頭列=名前
        date_map = _build_date_map(df, base_year, base_month,
                                   header_row_idx=1, date_start_col=1)
        if not date_map:
            print(f"  [{sheet_name}] 日付ヘッダを検出できず")
            continue

        schedules[i] = _parse_staff_rows_for_store(df, date_map, data_start_row=2)
        total = sum(len(v) for v in schedules[i].values())
        print(f"  [{sheet_name}] {len(schedules[i])} 人 / {total} 件のシフト")
    return schedules


# ============================================================
# メイン: DRY_RUN の場合は読み込み結果だけ出力して終了
# ============================================================
def main():
    dry = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")
    if dry:
        print("=== DRY_RUN モード: ベンリー操作は行いません ===")
        schedules = load_schedule()
        for i, sched in enumerate(schedules):
            print(f"\n--- 店舗{i+1} ({STORE_SHEETS[i]}) ---")
            for name, dates in sorted(sched.items()):
                if not dates:
                    continue
                for d in sorted(dates):
                    shift = dates[d]
                    label = "休み" if shift == "休み" else f"{shift[0]}-{shift[1]}"
                    print(f"  {name}  {d.strftime('%m/%d')}  {label}")
        print("\n=== DRY_RUN 完了 ===")
        return

    # 本番実行: Playwright を使うので main.py を遅延インポートして差し替え
    import main as main_module
    main_module.SPREADSHEET_ID = SPREADSHEET_ID
    main_module.load_schedule = load_schedule
    main_module.parse_time_cell = parse_time_cell
    main_module._normalize_name = _normalize_name
    main_module.main()


if __name__ == "__main__":
    main()
