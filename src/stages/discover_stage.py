import re
import time
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

BBREF_BASE = "https://www.basketball-reference.com"

# Valid season_end range for each league on bbref
_LEAGUE_START = {"BAA": 1947, "NBA": 1950, "ABA": 1968}
_LEAGUE_END   = {"BAA": 1949, "NBA": 2030, "ABA": 1976}

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def _fetch_html(page, url, page_delay):
    time.sleep(page_delay)
    page.goto(url, wait_until="domcontentloaded", timeout=30000)
    try:
        page.wait_for_selector("table", state="attached", timeout=8000)
    except Exception:
        pass
    return page.content()


def _parse_html(raw_html):
    return BeautifulSoup(raw_html, "lxml")


def _discover_months(page, league, season_end, page_delay):
    url = f"{BBREF_BASE}/leagues/{league}_{season_end}_games.html"
    soup = _parse_html(_fetch_html(page, url, page_delay))
    months = []
    for a in soup.select("div.filter a"):
        m = re.search(r"_games-(\w+)\.html", a.get("href", ""))
        if m:
            months.append(m.group(1))
    return months


def _schedule_urls_for_month(page, league, season_end, month, page_delay):
    url = f"{BBREF_BASE}/leagues/{league}_{season_end}_games-{month}.html"
    soup = _parse_html(_fetch_html(page, url, page_delay))
    table = soup.find("table", id="schedule")
    if table is None:
        return []
    rows = []
    is_playoff = False
    for tr in table.find("tbody").find_all("tr"):
        if "thead" in tr.get("class", []):
            # BBref inserts a "Playoffs" header row to separate playoff games
            if "playoff" in tr.get_text().lower():
                is_playoff = True
            continue
        td = tr.find("td", {"data-stat": "box_score_text"})
        if td and td.find("a"):
            href = td.find("a")["href"]
            game_id = href.split("/boxscores/")[-1].replace(".html", "")
            rows.append({
                "league": league,
                "season_end": season_end,
                "month": month,
                "game_id": game_id,
                "url": BBREF_BASE + href,
                "is_playoff": is_playoff,
            })
    return rows


def run(config, output_dir):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "game_urls.csv"

    leagues = config.get("leagues", ["NBA"])
    season_end_min = config.get("season_end_min", 1947)
    season_end_max = config.get("season_end_max", 1974)
    page_delay = float(config.get("discover_page_delay_s", config.get("page_delay_s", 2.0)))

    # Checkpointing: skip seasons already fully written
    completed_seasons: set = set()
    if out_path.exists():
        try:
            ex = pd.read_csv(out_path, usecols=["league", "season_end"])
            completed_seasons = set(zip(ex["league"], ex["season_end"].astype(int)))
            print(f"  Resuming — {len(completed_seasons)} seasons already on disk.")
        except Exception:
            pass

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent=_UA, locale="en-US", viewport={"width": 1280, "height": 800})
        page = context.new_page()

        for league in leagues:
            league_start = _LEAGUE_START.get(league, season_end_min)
            league_end = _LEAGUE_END.get(league, season_end_max)
            for season_end in reversed(range(max(season_end_min, league_start), min(season_end_max, league_end) + 1)):
                if (league, season_end) in completed_seasons:
                    print(f"    skip: {league} {season_end}")
                    continue

                print(f"    {league} {season_end} ...", end=" ", flush=True)
                try:
                    months = _discover_months(page, league, season_end, page_delay)
                    rows = []
                    for month in months:
                        rows.extend(_schedule_urls_for_month(page, league, season_end, month, page_delay))

                    if rows:
                        df = pd.DataFrame(rows)
                        for attempt in range(5):
                            try:
                                df.to_csv(out_path, mode="a", header=not out_path.exists(), index=False)
                                break
                            except PermissionError:
                                if attempt == 4:
                                    raise
                                time.sleep(2)
                        print(f"{len(rows)} games across {len(months)} months")
                    else:
                        print("0 games")
                except Exception as e:
                    print(f"ERROR: {e}")

        browser.close()

    if out_path.exists():
        total = sum(1 for _ in open(out_path)) - 1
        print(f"\n  {total} total games saved to {out_path}")
