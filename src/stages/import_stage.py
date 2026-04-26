import io
import re
import time
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from PIL import Image
from playwright.sync_api import sync_playwright

try:
    import pytesseract
    pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
    _TESSERACT_OK = True
except Exception:
    _TESSERACT_OK = False

BBREF_BASE = "https://www.basketball-reference.com"
_SCAN_BASE = "https://www.basketball-reference.com/req/202106291/images/boxscore-scans"

STAT_COLS = [
    "mp",
    "fg", "fga", "fg_pct",
    "fg3", "fg3a", "fg3_pct",
    "ft", "fta", "ft_pct",
    "orb", "drb", "trb",
    "ast", "stl", "blk", "tov", "pf", "pts",
    "plus_minus",
]

DNP_STRINGS = {
    "Did Not Play", "Did Not Dress", "Not With Team",
    "Inactive", "Player Suspended",
}

# Fixed output schema — ensures append-mode CSV columns are always aligned.
# OT columns will be NaN for regulation games.
_META_COLS = [
    "game_id", "url", "date", "league", "season_end", "is_playoff",
    "away_team", "home_team", "away_score", "home_score",
    "attendance", "attendance_source", "ocr_confidence",
    "team", "is_home", "player_id", "player_name", "is_team_total", "reason",
]
_PERIOD_COLS = [
    "Q1_Tm_Pts", "Q1_Opp_Pts",
    "Q2_Tm_Pts", "Q2_Opp_Pts",
    "Q3_Tm_Pts", "Q3_Opp_Pts",
    "Q4_Tm_Pts", "Q4_Opp_Pts",
    "OT1_Tm_Pts", "OT1_Opp_Pts",
    "OT2_Tm_Pts", "OT2_Opp_Pts",
    "OT3_Tm_Pts", "OT3_Opp_Pts",
]
OUTPUT_COLS = _META_COLS + _PERIOD_COLS + STAT_COLS

# Attendance patterns found in newspaper scan text
_ATT_PATTERNS = [
    r"[Aa]ttend\w*[^0-9]{0,10}([\d,]+)",   # "Attendance", OCR variants like "Attendonce"
    r"\bAtt\.?\s*[-—:]\s*([\d,]+)",
    r"\bA\s*[-—]\s*([\d,]+)",
    r"\bA\s+\-\s+([\d,]+)",
]


# ---------------------------------------------------------------------------
# Page fetching
# ---------------------------------------------------------------------------

def _fetch_html(page, url, page_delay):
    time.sleep(page_delay)
    page.goto(url, wait_until="domcontentloaded", timeout=30000)
    try:
        page.wait_for_selector("table#line_score", state="attached", timeout=8000)
    except Exception:
        try:
            page.wait_for_selector("table", state="attached", timeout=5000)
        except Exception:
            pass
    return page.content()


def _parse_html(raw_html):
    return BeautifulSoup(raw_html, "lxml")


# ---------------------------------------------------------------------------
# Attendance: HTML + OCR
# ---------------------------------------------------------------------------

def _attendance_from_html(soup):
    for strong in soup.find_all("strong"):
        if "Attendance" in strong.text:
            raw = strong.next_sibling
            if raw:
                val = str(raw).replace("\xa0", "").replace(",", "").strip()
                if val.isdigit():
                    return int(val)
    return None


def _attendance_from_scan(game_id, page, save_path=None):
    """Download the boxscore scan and OCR it for attendance.

    Returns (value: int|None, confidence: float|None).
    Confidence is the minimum per-word OCR confidence (0–1) across the digit
    tokens that make up the matched number.  A lower value means the OCR engine
    was less certain — useful as a quality flag in the clean stage.
    """
    if not _TESSERACT_OK:
        return None, None

    scan_url = f"{_SCAN_BASE}/{game_id}.jpg"
    try:
        response = page.context.request.fetch(scan_url, timeout=15000)
        if not response.ok:
            return None, None
        raw_bytes = response.body()
        if save_path is not None:
            save_path.write_bytes(raw_bytes)
        img = Image.open(io.BytesIO(raw_bytes)).convert("L")  # greyscale
    except Exception:
        return None, None

    try:
        data = pytesseract.image_to_data(
            img,
            output_type=pytesseract.Output.DICT,
            config="--psm 6",  # assume uniform block of text
        )
    except Exception:
        return None, None

    words = data["text"]
    confs = [int(c) for c in data["conf"]]

    # Build a searchable string and a parallel list of (word, conf) for valid tokens
    word_conf = [(w, c) for w, c in zip(words, confs) if w.strip() and c >= 0]
    full_text = " ".join(w for w, _ in word_conf)

    for pattern in _ATT_PATTERNS:
        m = re.search(pattern, full_text)
        if not m:
            continue
        num_str = m.group(1).replace(",", "").replace(".", "")
        if not (num_str.isdigit() and 50 <= int(num_str) <= 60_000):
            continue
        attendance = int(num_str)

        # Confidence: minimum over the digit tokens that match the number
        digit_confs = [
            c for w, c in word_conf
            if re.sub(r"[,.]", "", w).isdigit() and re.sub(r"[,.]", "", w) in num_str
        ]
        confidence = round(min(digit_confs) / 100.0, 3) if digit_confs else None
        return attendance, confidence

    return None, None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _game_id_to_date(game_id):
    m = re.match(r"(\d{4})(\d{2})(\d{2})\d+", game_id)
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}" if m else None


def _convert_mp(val):
    if val is None:
        return None
    try:
        parts = str(val).split(":")
        if len(parts) == 2:
            return round(int(parts[0]) + int(parts[1]) / 60, 4)
        return float(val)
    except (ValueError, AttributeError):
        return None


def _stat_to_period_label(stat):
    if re.match(r"^[1-4]$", stat):
        return f"Q{stat}"
    if re.match(r"^q\d+$", stat):
        return f"Q{stat[1:]}"
    if re.match(r"^\d+$", stat) and int(stat) >= 5:
        return f"OT{int(stat) - 4}"
    if stat.lower() == "ot":
        return "OT1"
    if re.match(r"^ot\d+$", stat.lower()):
        return f"OT{stat.lower()[2:]}"
    return None


def _parse_line_score(soup):
    table = soup.find("table", id="line_score")
    if table is None:
        return {"periods": [], "away": {}, "home": {}}
    thead = table.find("thead")
    if thead is None:
        return {"periods": [], "away": {}, "home": {}}

    header_row = thead.find_all("tr")[-1]
    period_stats, period_labels = [], []
    for cell in header_row.find_all(["th", "td"]):
        label = _stat_to_period_label(cell.get("data-stat", ""))
        if label:
            period_stats.append(cell.get("data-stat"))
            period_labels.append(label)

    tbody = table.find("tbody")
    if tbody is None:
        return {"periods": period_labels, "away": {}, "home": {}}

    result = {"periods": period_labels, "away": {}, "home": {}}
    for side, row in zip(["away", "home"], tbody.find_all("tr")):
        for stat, label in zip(period_stats, period_labels):
            td = row.find(["th", "td"], {"data-stat": stat})
            val = td.text.strip() if td else ""
            result[side][label] = int(val) if val.lstrip("-").isdigit() else None
    return result


def _parse_box_table(table, game_meta, line_scores):
    m = re.match(r"box-(.+)-game-basic", table.get("id", ""))
    team_abbrev = m.group(1) if m else "UNK"

    if team_abbrev == game_meta.get("away_team"):
        tm_scores, opp_scores = line_scores["away"], line_scores["home"]
    else:
        tm_scores, opp_scores = line_scores["home"], line_scores["away"]

    rows = []
    for section in [s for s in [table.find("tbody"), table.find("tfoot")] if s]:
        for tr in section.find_all("tr"):
            if "thead" in tr.get("class", []):
                continue
            player_th = tr.find("th", {"data-stat": "player"})
            if player_th is None:
                continue
            player_a = player_th.find("a")
            if player_a is not None:
                player_id = player_a["href"].split("/")[-1].replace(".html", "")
                player_name = player_a.text.strip()
                is_team_total = False
            else:
                label = player_th.text.strip()
                if not label:
                    continue
                player_id, player_name, is_team_total = "TEAM_TOTAL", label, True

            reason_td = tr.find("td", {"data-stat": "reason"})
            row = {
                **game_meta,
                "team": team_abbrev,
                "is_home": team_abbrev == game_meta.get("home_team"),
                "player_id": player_id,
                "player_name": player_name,
                "is_team_total": is_team_total,
                "reason": reason_td.text.strip() if reason_td else None,
            }
            for period in line_scores["periods"]:
                row[f"{period}_Tm_Pts"] = tm_scores.get(period)
                row[f"{period}_Opp_Pts"] = opp_scores.get(period)
            for stat in STAT_COLS:
                td = tr.find("td", {"data-stat": stat})
                val = td.text.strip() if td else ""
                row[stat] = None if (val == "" or val in DNP_STRINGS) else val
            rows.append(row)
    return rows


def _fetch_box_score(page, url, page_delay, games_dir=None):
    raw_html = _fetch_html(page, url, page_delay)
    soup = _parse_html(raw_html)
    game_id = url.split("/boxscores/")[-1].replace(".html", "")

    if games_dir is not None:
        (games_dir / f"{game_id}.html").write_text(raw_html, encoding="utf-8")

    scores = [d.text.strip() for d in soup.select("div.scorebox div.score")]
    basic_tables = soup.find_all("table", id=re.compile(r"box-.+-game-basic"))
    team_abbrevs = [
        re.match(r"box-(.+)-game-basic", t.get("id", "")).group(1)
        for t in basic_tables
        if re.match(r"box-(.+)-game-basic", t.get("id", ""))
    ]

    # Attendance: HTML first, OCR fallback
    attendance = _attendance_from_html(soup)
    if attendance is not None:
        att_source, ocr_conf = "html", None
    else:
        img_path = (games_dir / f"{game_id}.jpg") if games_dir is not None else None
        attendance, ocr_conf = _attendance_from_scan(game_id, page, save_path=img_path)
        att_source = "ocr" if attendance is not None else None

    game_meta = {
        "game_id": game_id,
        "url": url,
        "date": _game_id_to_date(game_id),
        "away_team": team_abbrevs[0] if team_abbrevs else None,
        "home_team": team_abbrevs[1] if len(team_abbrevs) > 1 else None,
        "away_score": scores[0] if scores else None,
        "home_score": scores[1] if len(scores) > 1 else None,
        "attendance": attendance,
        "attendance_source": att_source,
        "ocr_confidence": ocr_conf,
    }

    line_scores = _parse_line_score(soup)
    rows = []
    for table in basic_tables:
        rows.extend(_parse_box_table(table, game_meta, line_scores))
    return rows


# ---------------------------------------------------------------------------
# Pipeline entry point
# ---------------------------------------------------------------------------

def run(config, output_dir):
    output_dir = Path(output_dir)
    games_dir = output_dir / "games"
    games_dir.mkdir(parents=True, exist_ok=True)

    page_delay = float(config.get("page_delay_s", 4.0))

    # Load game URLs produced by discover stage
    urls_path = output_dir / "game_urls.csv"
    if not urls_path.exists():
        print(f"  ERROR: {urls_path} not found. Run discover stage first.")
        return

    urls_df = pd.read_csv(urls_path)

    # Filter by game_type: "playoff", "regular", or "all"
    game_type = config.get("game_type", "all")
    if game_type == "playoff":
        urls_df = urls_df[urls_df["is_playoff"] == True]
    elif game_type == "regular":
        urls_df = urls_df[urls_df["is_playoff"] == False]

    filtered_df = urls_df.sort_values("season_end", ascending=False).reset_index(drop=True)

    # Checkpointing: a game is done if its individual file already exists
    completed_ids = {p.stem for p in games_dir.glob("*.csv")}
    remaining = len(filtered_df) - len(filtered_df[filtered_df["game_id"].isin(completed_ids)])
    print(f"  {len(filtered_df)} {game_type} games across {filtered_df['season_end'].nunique()} seasons "
          f"({len(completed_ids)} done, {remaining} remaining)")

    _UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=_UA,
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        for _, game_row in filtered_df.iterrows():
            game_id = game_row["game_id"]
            url = game_row["url"]
            league = game_row["league"]
            season_end = game_row["season_end"]
            game_path = games_dir / f"{game_id}.csv"

            if game_id in completed_ids:
                print(f"    skip (done): {game_id}")
                continue

            print(f"    {league} {season_end} {game_type}: {url}")
            try:
                rows = _fetch_box_score(page, url, page_delay, games_dir=games_dir)
                if not rows:
                    continue
                for r in rows:
                    r["league"] = league
                    r["season_end"] = season_end
                    r["is_playoff"] = game_row["is_playoff"]
                game_df = pd.DataFrame(rows)
                game_df["mp"] = game_df["mp"].apply(_convert_mp)
                game_df = game_df.reindex(columns=OUTPUT_COLS)
                for attempt in range(5):
                    try:
                        game_df.to_csv(game_path, index=False)
                        break
                    except PermissionError:
                        if attempt == 4:
                            raise
                        time.sleep(2)
                completed_ids.add(game_id)
                att = rows[0].get("attendance")
                src = rows[0].get("attendance_source")
                conf = rows[0].get("ocr_confidence")
                att_str = f"att={att}({src}" + (f",conf={conf:.2f})" if conf else ")") if att else "att=None"
                print(f"      -> {len(rows)} rows  {att_str}")
            except Exception as e:
                print(f"      ERROR: {e}")

        browser.close()

    total = len(list(games_dir.glob("*.csv")))
    print(f"\n  {total} game files in {games_dir}")


def _schedule_game_urls(page, league, season_end, month, max_games, page_delay):
    url = f"{BBREF_BASE}/leagues/{league}_{season_end}_games-{month}.html"
    print(f"    schedule: {url}")
    soup = _parse_html(_fetch_html(page, url, page_delay))
    table = soup.find("table", id="schedule")
    if table is None:
        print("    WARNING: schedule table not found")
        return []
    urls = []
    for row in table.find("tbody").find_all("tr"):
        if "thead" in row.get("class", []):
            continue
        td = row.find("td", {"data-stat": "box_score_text"})
        if td and td.find("a"):
            urls.append(BBREF_BASE + td.find("a")["href"])
            if len(urls) >= max_games:
                break
    return urls
