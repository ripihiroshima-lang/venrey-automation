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
from datetime import datetime, date, timedelta, timezone

import pandas as pd

# ============================================================
# テスト用シート固有の設定
# ============================================================
# 本番月次SS（C-036連携先）。JST現在月から自動でSSを選択。
# 新月分のSSが用意されたら下の MONTHLY_SHEETS に1行追加するだけで対応可能。
# 緊急時は環境変数 BENRY_TEST_SHEET_ID で上書き可能（一時的な強制指定用）。
MONTHLY_SHEETS = {
    (2026,  4): "1y33e0FlbS2R9d-iMQqaSm29rtYtl1JIcR1PpERwG2W4",
    (2026,  5): "1XTmmkZP6k6PIhZ7RPHD75ClC_gfFA11U960wvCvEuB0",
    (2026,  6): "1yizaAM_aQFaepv0kYlKBZY7uDUDsJA_9rwSn7hrDqKQ",
    (2026,  7): "1k4FVGkUkqR1HUaroC0bH-REaAy8pEM3uFjO3b202e9o",
    (2026,  8): "1en-dlxUDLmSEmSPp2mCBgYDYfDfmfKCf7GlPFAN8GSg",
    (2026,  9): "1UeVn4llROiPOnISZ86KN3hXp0f4_RCARL07iEflBDNI",
    (2026, 10): "19FbRRmAhQONKhsTK3jeHLJV_WLTcpmiP1n64ykAXrOM",
    (2026, 11): "1XalqXt1sfswmPasUY2jMtTXduA6VR7JE-ieBDFKX06I",
    (2026, 12): "1tuTfLTt1D1yXp8e86IICYkf4iAo9yHlJpr1OVNfZcQY",
}
JST = timezone(timedelta(hours=9))
_now_jst = datetime.now(JST)
_FALLBACK_SHEET_ID = "1y33e0FlbS2R9d-iMQqaSm29rtYtl1JIcR1PpERwG2W4"  # 未登録月のフォールバック
DEFAULT_TEST_SHEET_ID = MONTHLY_SHEETS.get((_now_jst.year, _now_jst.month), _FALLBACK_SHEET_ID)
SPREADSHEET_ID = os.environ.get("BENRY_TEST_SHEET_ID") or DEFAULT_TEST_SHEET_ID
print(f"[INFO] {_now_jst.strftime('%Y-%m')} (JST) -> SHEET_ID={SPREADSHEET_ID}", flush=True)

# 店舗インデックス → タブ名（STORES[i] に対応）
STORE_SHEETS = ["CREA", "ふわもこ"]

# 集計行マーカー（行頭または含まれたらスキップ）
# 「📊出勤人数」「🏪店欠」「📅事前欠勤」のような絵文字付き集計行も含める
AGGREGATE_MARKERS = (
    "📊", "🏪", "📅",
    "出勤人数", "合計",
    "店欠", "前欠", "当欠", "事前欠勤", "当日欠勤", "店都合",
)

# 「受」末尾シフトの終了時刻に加算する分数（送迎・片付け込みの実退店時刻）
# 例: 「19-24受」→ 終了 24:00 + 1:30 = 25:30
UKE_OVERTIME_MIN = 90


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
# シフトセル解析（C-036フォーマット + 旧 *XXX 形式の両対応）
# ============================================================
def _parse_time_part(raw):
    """「12」「1230」「12:30」を (hour, minute) に変換。失敗時 (None, None)。"""
    raw = raw.strip().lstrip("*")
    if not raw:
        return None, None
    if ":" in raw:
        parts = raw.split(":", 1)
        if not (parts[0].isdigit() and parts[1].isdigit()):
            return None, None
        return int(parts[0]), int(parts[1])
    if not raw.isdigit():
        return None, None
    if len(raw) <= 2:
        return int(raw), 0
    return int(raw[:-2]), int(raw[-2:])


def _add_minutes(eh, em, add_min):
    em += add_min
    while em >= 60:
        em -= 60
        eh += 1
    return eh, em


def parse_time_cell(cell_value):
    """
    テスト用シートのシフトセル値を (開始, 終了) にパースする。

    対応フォーマット:
      「19-24受」          → ("19:00", "25:30")  ★受=終了+1:30★
      「19-24上」          → ("19:00", "24:00")  上=そのまま
      「19-24*130」        → ("19:00", "25:30")  旧形式 *XXX 互換
      「19-3受」           → ("19:00", "28:30")  深夜跨ぎ + 受
      「12-15:30上」       → ("12:00", "15:30")
      「13:00〜22:50上」    → ("13:00", "22:50")
      「18:00 〜 24:00 上」 → ("18:00", "24:00")
      「21:30-27上」       → ("21:30", "27:00")  既に翌日表記
      「OFF / 当欠 / 前欠 / 店欠」 → "休み"
      ""                  → None
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

    # 全角数字を半角に
    s = s.translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    # 区切りを「-」に統一
    s = s.replace("～", "-").replace("〜", "-").replace("~", "-").replace("ー", "-").replace("―", "-")
    # 空白・全角空白を除去
    s = re.sub(r'\s+', '', s).replace("　", "")

    if not re.search(r"\d", s):
        return None

    # パターン1: START-END*OVERTIME（旧形式 *NNN は終了時刻を上書き）
    # 例: 19-24*130 → eh=01:30 → +24 → 25:30
    m = re.match(r'^(\d+(?::\d+)?)-(\d+(?::\d+)?)\*(\d{1,4})', s)
    if m:
        sh, sm = _parse_time_part(m.group(1))
        ot_raw = m.group(3).zfill(4) if len(m.group(3)) > 2 else m.group(3)
        eh, em = _parse_time_part(ot_raw)
        if sh is None or eh is None:
            return None
        if sh >= 24:
            sh -= 24
        if eh < 6:
            eh += 24
        return f"{sh:02d}:{sm:02d}", f"{eh:02d}:{em:02d}"

    # パターン2: START-END[上/受](注記)
    m = re.match(r'^([\d:]+)-([\d:]+)([上受])?', s)
    if m:
        sh, sm = _parse_time_part(m.group(1))
        eh, em = _parse_time_part(m.group(2))
        suffix = m.group(3) or ""
        if sh is None or eh is None:
            return None
        if sh >= 24:
            sh -= 24
        # 深夜跨ぎ: 終了が開始より小さく、24未満なら +24
        if eh < sh and eh < 24:
            eh += 24
        # 「受」末尾は終了時刻に+1:30（送迎・片付け込み）
        if suffix == "受":
            eh, em = _add_minutes(eh, em, UKE_OVERTIME_MIN)
        # 範囲チェック
        if not (0 <= sh < 24 and 0 <= sm < 60 and 0 <= em < 60):
            return None
        if eh > 32:
            return None
        return f"{sh:02d}:{sm:02d}", f"{eh:02d}:{em:02d}"

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
# C-040 v6.1: ベンリー反映成功後のセル色付け
# ----------------------------------------------------------
# 「色付き=ベンリー反映済」「白=未反映/窓外/空」の意味で統一する。
# shift_reflect.js の SS書込時の色付けは v6.1 で廃止済み。
# main_test.py がベンリー反映成功(main_module.main()完了後)に
# 窓内 (today〜today+6) の全スタッフ・全日付セルの format を再計算する。
# ============================================================

# すいさん仕様 (2026-04-30) - 反映済セルの色マッピング
_CELL_STYLES = {
    'uke':      {'bg': '#C8E6C9', 'font': '#1B5E20', 'bold': True},   # 受 - 薄緑/濃緑
    'up':       {'bg': '#BBDEFB', 'font': '#0D47A1', 'bold': True},   # 上 - 薄青/濃青
    'akke':     {'bg': '#fecaca', 'font': '#991b1b', 'bold': True},   # 当欠
    'zenke':    {'bg': '#fff1f2', 'font': '#be123c', 'bold': True},   # 前欠
    'tenketsu': {'bg': '#fed7aa', 'font': '#9a3412', 'bold': True},   # 店欠 / 当欠店
    'zenten':   {'bg': '#fef9c3', 'font': '#854d0e', 'bold': True},   # 前欠店
    'off':      {'bg': '#f1f5f9', 'font': '#94a3b8', 'bold': False},  # OFF
    'white':    {'bg': '#ffffff', 'font': '#000000', 'bold': True},   # 未反映/空
}


def _hex_to_rgb01(hex_color):
    h = hex_color.replace('#', '')
    return {
        'red':   int(h[0:2], 16) / 255.0,
        'green': int(h[2:4], 16) / 255.0,
        'blue':  int(h[4:6], 16) / 255.0,
    }


def _get_cell_style(value):
    v = str(value or '').strip()
    if not v or v == 'nan':
        return _CELL_STYLES['white']
    up = v.upper()
    if v == '当欠':                  return _CELL_STYLES['akke']
    if v == '前欠':                  return _CELL_STYLES['zenke']
    if v == '店欠' or v == '当欠店': return _CELL_STYLES['tenketsu']
    if v == '前欠店':                return _CELL_STYLES['zenten']
    if up == 'OFF':                  return _CELL_STYLES['off']
    if v.endswith('上'):             return _CELL_STYLES['up']
    if v.endswith('受'):             return _CELL_STYLES['uke']
    return _CELL_STYLES['white']


def _build_format_request(sheet_id, row_idx, col_idx, style):
    """1セル分の repeatCell リクエスト構築 (row/col_idx は 0-based)。"""
    return {
        'repeatCell': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': row_idx,
                'endRowIndex': row_idx + 1,
                'startColumnIndex': col_idx,
                'endColumnIndex': col_idx + 1,
            },
            'cell': {
                'userEnteredFormat': {
                    'backgroundColor': _hex_to_rgb01(style['bg']),
                    'textFormat': {
                        'foregroundColor': _hex_to_rgb01(style['font']),
                        'bold': bool(style['bold']),
                    },
                    'horizontalAlignment': 'CENTER',
                    'verticalAlignment': 'MIDDLE',
                },
            },
            'fields': 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)',
        }
    }


def update_cell_colors_after_reflect():
    """ベンリー反映成功後、SPREADSHEET_ID の窓内セル format を一括更新する。
    エラーが出ても致命的でないため、上位で try/except して続行可能にすること。"""
    today = datetime.now().date()
    week_end = today + timedelta(days=6)
    in_window = lambda d: today <= d <= week_end

    # SS メタ取得 (sheet_id 取得用)
    access_token = _get_access_token()
    meta_url = f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}"
    req = urllib.request.Request(meta_url, headers={"Authorization": f"Bearer {access_token}"})
    with urllib.request.urlopen(req) as r:
        meta = json.load(r)
    sheet_title_to_id = {
        s['properties']['title']: s['properties']['sheetId']
        for s in meta.get('sheets', [])
    }

    requests_list = []
    for store_name in STORE_SHEETS:
        if store_name not in sheet_title_to_id:
            continue
        sheet_id = sheet_title_to_id[store_name]
        df = _fetch_sheet_df(store_name)
        if df is None or df.shape[0] < 3:
            continue

        # タイトルから年月検出 (load_schedule と同じロジック)
        title_cell = str(df.iloc[0, 0]) if df.shape[0] > 0 and df.shape[1] > 0 else ""
        m = re.search(r'(\d{4})年(\d{1,2})月', title_cell)
        if m:
            base_year, base_month = int(m.group(1)), int(m.group(2))
        else:
            base_year, base_month = today.year, today.month

        date_map = _build_date_map(df, base_year, base_month, header_row_idx=1, date_start_col=1)
        if not date_map:
            continue

        window_cols = [(col_idx, d) for col_idx, d in date_map.items() if in_window(d)]
        if not window_cols:
            continue

        for row_idx in range(2, df.shape[0]):
            raw_name = str(df.iloc[row_idx, 0]).strip()
            if not raw_name or raw_name == "nan":
                continue
            if any(marker in raw_name for marker in AGGREGATE_MARKERS):
                continue
            if not re.search(r'[\w぀-ヿ一-鿿]', raw_name):
                continue
            for col_idx, _d in window_cols:
                if col_idx >= df.shape[1]:
                    continue
                value = str(df.iloc[row_idx, col_idx]).strip()
                style = _get_cell_style(value)
                requests_list.append(_build_format_request(sheet_id, row_idx, col_idx, style))

    if not requests_list:
        print("[Color] 色付け対象なし")
        return

    body = json.dumps({'requests': requests_list}).encode()
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}:batchUpdate"
    req = urllib.request.Request(url, data=body, method='POST', headers={
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    })
    with urllib.request.urlopen(req) as r:
        json.load(r)
    print(f"[Color] {len(requests_list)} セルの format を更新")


# ============================================================
# 差分検知: 前回反映時のスケジュールと比較して、変更されたスタッフのみ抽出
# ============================================================
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".state", "last_state.json")


def _serialize_schedules(schedules):
    """シフトデータを JSON 保存可能な形式に変換。
    日付は ISO形式 'YYYY-MM-DD' 文字列、シフトは 'HH:MM-HH:MM' or '休み' に正規化。"""
    out = {}
    for i, store_name in enumerate(STORE_SHEETS):
        store_data = {}
        for name, date_map in schedules[i].items():
            shifts = {}
            for d, val in date_map.items():
                key = d.isoformat() if hasattr(d, "isoformat") else str(d)
                if val == "休み":
                    shifts[key] = "休み"
                elif isinstance(val, (list, tuple)) and len(val) == 2:
                    shifts[key] = f"{val[0]}-{val[1]}"
                else:
                    shifts[key] = str(val)
            store_data[name] = shifts
        out[store_name] = store_data
    return {"stores": out, "version": 1}


def _load_last_state():
    if not os.path.exists(STATE_FILE):
        return None
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"  state読込エラー: {e}")
        return None


def _save_state(state):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def detect_changed_staff(schedules):
    """前回 state と比較して、今週分に変更があったスタッフ名のリストを返す。

    Returns:
        set of staff names (CREA + ふわもこ 統合)
        前回 state がない場合は None（→ 全員対象として扱う）
    """
    today = datetime.now().date()
    week_end = today + timedelta(days=6)
    in_week = lambda iso: today.isoformat() <= iso <= week_end.isoformat()

    current = _serialize_schedules(schedules)
    last = _load_last_state()
    if last is None:
        print("  前回stateなし → 全員対象（初回 or cache miss）")
        return None  # 全員対象

    changed = set()
    for store_name in STORE_SHEETS:
        cur_store = current["stores"].get(store_name, {})
        last_store = last.get("stores", {}).get(store_name, {})
        all_names = set(cur_store.keys()) | set(last_store.keys())
        for name in all_names:
            cur_shifts = {k: v for k, v in cur_store.get(name, {}).items() if in_week(k)}
            last_shifts = {k: v for k, v in last_store.get(name, {}).items() if in_week(k)}
            if cur_shifts != last_shifts:
                changed.add(name)
    return changed


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

    # 差分検知: SELECTED_STAFF が手動指定なら尊重、 未指定なら自動検知
    selected_raw = os.environ.get("SELECTED_STAFF", "").strip()
    schedules_for_diff = load_schedule()

    if selected_raw:
        print(f"\n手動指定スタッフ: {selected_raw}")
    else:
        changed = detect_changed_staff(schedules_for_diff)
        if changed is None:
            print("→ 全員対象で進行")
        elif not changed:
            print("→ 今週分の変更なし。スキップして終了")
            return
        else:
            staff_list = sorted(changed)
            print(f"→ 今週分の変更ありスタッフ: {staff_list}")
            os.environ["SELECTED_STAFF"] = ",".join(staff_list)
            selected_raw = os.environ["SELECTED_STAFF"]

    # 本番実行: Playwright を使うので main.py を遅延インポートして差し替え
    import main as main_module
    main_module.SPREADSHEET_ID = SPREADSHEET_ID
    main_module.load_schedule = load_schedule
    main_module.parse_time_cell = parse_time_cell
    main_module._normalize_name = _normalize_name
    main_module.main()

    # ★ C-040 v6.1: ベンリー反映成功後、SS の窓内セルを色付け更新
    try:
        update_cell_colors_after_reflect()
    except Exception as e:
        print(f"  色付けエラー (致命的でないので続行): {e}")

    # 反映が成功したら state を更新（次回の差分判定の基準にする）
    try:
        _save_state(_serialize_schedules(schedules_for_diff))
        print(f"\n  state を保存: {STATE_FILE}")
    except Exception as e:
        print(f"  state 保存エラー: {e}")

    # ベンリー反映後、サイトへ自動配信（SKIP_SITE_SYNC=1 で無効化）
    if os.environ.get("SKIP_SITE_SYNC", "").lower() in ("1", "true", "yes"):
        print("\nSKIP_SITE_SYNC 設定によりサイト配信をスキップします")
        return

    # 直前のベンリー反映を確実に保存させるため、少し待つ
    import time as _time
    print("\nサイトへの配信のため 30 秒待機（ベンリー側の保存完了を待つ）...")
    _time.sleep(30)

    # サイト配信は店舗ごと（CREA / ふわもこ）に実行する必要がある
    import subprocess, sys as _sys
    selected_raw = os.environ.get("SELECTED_STAFF", "").strip()
    sync_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sync_to_sites.py")
    if not os.path.exists(sync_script):
        print(f"⚠ {sync_script} が見つかりません。サイト配信をスキップします")
        return

    # 各店舗のスタッフだけ抽出してサイト配信
    schedules = load_schedule()
    if selected_raw:
        target_set = set(_normalize_name(n) for n in selected_raw.split(",") if n.strip())
        schedules = [{k: v for k, v in s.items() if k in target_set} for s in schedules]

    store_keys = ["crea", "fuwamoko"]
    for i, sched in enumerate(schedules):
        if not sched:
            continue
        store_key = store_keys[i]
        names = ",".join(sorted(sched.keys()))
        print(f"\n=== サイト配信開始: store={store_key}, staff={names} ===")
        env = {**os.environ, "SELECTED_STAFF": names, "STORE": store_key, "HEADLESS": "true"}
        try:
            r = subprocess.run([_sys.executable, sync_script], env=env, timeout=600)
            if r.returncode != 0:
                print(f"⚠ サイト配信が異常終了 (exit={r.returncode})")
        except Exception as e:
            print(f"⚠ サイト配信エラー: {e}")


if __name__ == "__main__":
    main()
