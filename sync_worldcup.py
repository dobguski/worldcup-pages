#!/usr/bin/env python3
"""
2026 FIFA World Cup Auto-Sync Engine
=====================================
持续监控世界杯比赛，赛果一出即同步到 Football.TXT 并提交推送。

数据源:
  - ESPN API (免费, 无需key) - 实时比分、比赛状态
  - TheSportsDB API (免费key=3) - 详细赛果、进球者

用法:
  python sync_worldcup.py          # 单次同步
  python sync_worldcup.py --watch  # 持续监控 + Web看板
  python sync_worldcup.py --serve  # 仅Web看板服务器
  python sync_worldcup.py --tui    # 终端TUI看板 (rich库)
"""

import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ============================================================
# CONFIG
# ============================================================
REPO_DIR = Path(__file__).parent
CUP_TXT = REPO_DIR / "2026--usa" / "cup.txt"
FINALS_TXT = REPO_DIR / "2026--usa" / "cup_finals.txt"
DATA_JSON = REPO_DIR / "match_data.json"
STANDINGS_JSON = REPO_DIR / "standings.json"
TEAM_NAMES_JSON = REPO_DIR / "team_names.json"
TEAMS_JSON = REPO_DIR / "teams.json"

ESPN_API = "https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/scoreboard?dates=20260611-20260719"
TSDB_API = "https://www.thesportsdb.com/api/v1/json/3/eventsseason.php?id=4429&s=2026"
TSDB_PAST_API = "https://www.thesportsdb.com/api/v1/json/3/eventspastleague.php?id=4429"
TSDB_NEXT_API = "https://www.thesportsdb.com/api/v1/json/3/eventsnextleague.php?id=4429"
TSDB_EVENT_API = "https://www.thesportsdb.com/api/v1/json/3/lookupevent.php?id="
TSDB_DAY_API = "https://www.thesportsdb.com/api/v1/json/3/eventsday.php?d={date}&s=Soccer"
FIFA_CALENDAR_API = "https://api.fifa.com/api/v3/calendar/matches?from=2026-06-10T00:00:00Z&to=2026-07-20T23:59:59Z&count=200"

# 2026 World Cup timezones used in Football.TXT
# EDT = UTC-4, CDT = UTC-5, MDT = UTC-6, PDT = UTC-7
TZ_OFFSETS = {
    "Mexico City": "UTC-6",
    "Guadalajara (Zapopan)": "UTC-6",
    "Monterrey (Guadalupe)": "UTC-6",
    "Atlanta": "UTC-4",
    "Boston (Foxborough)": "UTC-4",
    "Dallas (Arlington)": "UTC-5",
    "Houston": "UTC-5",
    "Kansas City": "UTC-5",
    "Los Angeles (Inglewood)": "UTC-7",
    "Miami (Miami Gardens)": "UTC-4",
    "New York/New Jersey (East Rutherford)": "UTC-4",
    "Philadelphia": "UTC-4",
    "San Francisco Bay Area (Santa Clara)": "UTC-7",
    "Seattle": "UTC-7",
    "Toronto": "UTC-4",
    "Vancouver": "UTC-7",
}

POLL_INTERVAL = 15   # seconds between API polls during live matches
WATCH_INTERVAL = 30   # seconds between full sync cycles


# ============================================================
# API FETCHERS
# ============================================================
def fetch_json(url: str, retries: int = 1, backoff: float = 1.0) -> dict | None:
    """Fetch JSON from URL with retries, gzip support, and SSL fallback."""
    import ssl, gzip
    ssl_ctx = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
            })
            opener_args = {}
            if ssl_ctx is not None:
                opener_args['context'] = ssl_ctx
            with urllib.request.urlopen(req, timeout=15, **opener_args) as resp:
                raw = resp.read()
                if raw[:2] == b'\x1f\x8b':  # gzip magic bytes
                    raw = gzip.decompress(raw)
                return json.loads(raw.decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
                continue
            if attempt >= retries - 1:
                print(f"  [WARN] HTTP {e.code} on {url[:60]}...")
        except Exception as e:
            if 'SSL' in str(e).upper() and ssl_ctx is None:
                ssl_ctx = ssl.create_default_context()
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE
                time.sleep(1)
                continue
            if attempt >= retries - 1:
                print(f"  [WARN] {url[:60]}...: {e}")
    return None


def fetch_espn_matches() -> list[dict]:
    """Fetch current match data from ESPN public API."""
    data = fetch_json(ESPN_API)
    if not data:
        return []
    matches = []
    for event in data.get("events", []):
        comp = event.get("competitions", [{}])[0]
        if not comp:
            continue
        home_team = away_team = None
        home_score = away_score = None
        competitors = comp.get("competitors", [])
        for c in competitors:
            if c.get("homeAway") == "home":
                home_team = c.get("team", {}).get("displayName", "")
                home_score = c.get("score")
            else:
                away_team = c.get("team", {}).get("displayName", "")
                away_score = c.get("score")

        status_info = event.get("status", {})
        status_type = status_info.get("type", {})
        status_name = status_type.get("name", "STATUS_SCHEDULED")
        status_detail = status_type.get("detail", "")
        period = status_info.get("period", 0)
        clock = status_info.get("displayClock", "")

        matches.append({
            "id": event.get("id", ""),
            "date": event.get("date", "")[:10],
            "time_utc": event.get("date", ""),
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
            "status": status_name,
            "status_detail": status_detail,
            "period": period,
            "clock": clock,
            "venue": comp.get("venue", {}).get("fullName", ""),
            "source": "espn",
        })
    return matches


def fetch_thesportsdb_matches() -> list[dict]:
    """Fetch all 2026 World Cup matches from TheSportsDB (multiple endpoints for free key coverage)."""
    matches = []
    seen = set()
    def add_events(data, status_hint=""):
        if not data or "events" not in data:
            return
        for e in data["events"]:
            eid = e.get("idEvent", "")
            if eid in seen: continue
            seen.add(eid)
            hs = e.get("intHomeScore")
            aws = e.get("intAwayScore")
            matches.append({
                "id": eid,
                "date": e.get("dateEvent", ""),
                "home_team": e.get("strHomeTeam", ""),
                "away_team": e.get("strAwayTeam", ""),
                "home_score": int(hs) if hs is not None and hs != "" else None,
                "away_score": int(aws) if aws is not None and aws != "" else None,
                "status": e.get("strStatus", status_hint),
                "venue": e.get("strVenue", ""),
                "round": e.get("intRound", ""),
                "source": "thesportsdb",
            })

    # Season events (main endpoint, but limited on free key)
    add_events(fetch_json(TSDB_API))
    # Past results (recently finished)
    add_events(fetch_json(TSDB_PAST_API), "FT")
    # Upcoming matches
    add_events(fetch_json(TSDB_NEXT_API), "Scheduled")
    # Recent days (last 3 days of matches)
    from datetime import date, timedelta
    for i in range(4):
        d = (date.today() - timedelta(days=i)).isoformat()
        add_events(fetch_json(TSDB_DAY_API.format(date=d)), "")
    return matches


def fetch_pm_odds() -> dict | None:
    """Fetch Polymarket odds from Gamma API event/30615 (World Cup Winner).
    Updates polymarket.json daily. Returns updated dict or None if fetch fails."""
    import datetime
    pm_path = REPO_DIR / 'polymarket.json'
    try:
        pm = json.loads(pm_path.read_text(encoding='utf-8'))
    except Exception:
        pm = {'matches': {}, 'champion_odds': {}, 'updated': '', 'source': ''}

    last_update = pm.get('champion_odds_updated', '')
    today = datetime.date.today().isoformat()
    if last_update.startswith(today):
        return pm

    try:
        url = 'https://gamma-api.polymarket.com/events/30615'
        event = fetch_json(url, retries=2, backoff=2.0)
        if event:
            champion_odds = {}
            for m in event.get('markets', []):
                q = m.get('question', ''); outcomes = m.get('outcomes', [])
                prices = m.get('outcomePrices', [])
                if isinstance(outcomes, str): outcomes = json.loads(outcomes)
                if isinstance(prices, str): prices = json.loads(prices)
                for o in outcomes:
                    label = o.get('outcome', o) if isinstance(o, dict) else o
                    if label.lower() == 'yes':
                        import re
                        team_match = re.search(r'Will (.+?) win the 2026', q)
                        idx = list(outcomes).index(o)
                        prob = float(prices[idx]) if idx < len(prices) else 0
                        if team_match and prob > 0.001:
                            champion_odds[team_match.group(1).strip()] = round(prob, 4)
            if champion_odds:
                pm['champion_odds'] = champion_odds
                pm['champion_odds_updated'] = today
                vol = float(event.get('volume', 0))
                pm['champion_odds_volume'] = f'${vol:,.0f}'
                pm['source'] = 'Polymarket Gamma API (event/30615 daily)'
                pm['updated'] = datetime.datetime.now().isoformat()
    except Exception as e:
        print(f'  [PM] {e}')

    pm_path.write_text(json.dumps(pm, ensure_ascii=False, indent=2), encoding='utf-8')
    n_odds = len(pm.get('champion_odds', {}))
    print(f'  [PM] {n_odds} teams updated')
    return pm


def fetch_fifa_matches() -> list[dict]:
    """Fetch 2026 World Cup matches from FIFA calendar API (tertiary source)."""
    data = fetch_json(FIFA_CALENDAR_API, retries=1, backoff=1.0)
    if not data or 'Results' not in data:
        return []
    matches = []
    wc_keywords = ['world cup', 'fifa world', 'cop do mundo', 'coupe du monde', 'copa mundial', 'weltmeisterschaft']
    for m in data['Results']:
        comp = ' '.join([c.get('Description','') for c in m.get('CompetitionName', [])]).lower()
        season = ' '.join([s.get('Description','') for s in m.get('SeasonName', [])]).lower()
        if not any(kw in comp or kw in season for kw in wc_keywords):
            continue  # Skip non-World Cup matches
        h = m.get('Home', {})
        a = m.get('Away', {})
        hs = h.get('Score')
        aws = a.get('Score')
        matches.append({
            'id': m.get('IdMatch', ''),
            'date': (m.get('Date', ''))[:10],
            'home_team': (h.get('TeamName', [{}]) or [{}])[0].get('Description', '') if h.get('TeamName') else '',
            'away_team': (a.get('TeamName', [{}]) or [{}])[0].get('Description', '') if a.get('TeamName') else '',
            'home_score': int(hs) if hs is not None and hs != '' else None,
            'away_score': int(aws) if aws is not None and aws != '' else None,
            'status': 'FT' if (hs is not None and hs != '') else 'Scheduled',
            'venue': m.get('Stadium', {}).get('Name', [{}])[0].get('Description', '') if m.get('Stadium') else '',
            'source': 'fifa',
        })
    return matches

def fetch_event_details(event_id: str) -> dict | None:
    """Fetch detailed event data including goal scorers."""
    data = fetch_json(TSDB_EVENT_API + event_id)
    if not data or "events" not in data:
        return None
    return data["events"][0]


# ============================================================
# NAME NORMALIZATION (different APIs use different names)
# ============================================================
NAME_MAP = {
    # ESPN -> Football.TXT name
    "Korea Republic": "South Korea",
    "Czechia": "Czech Republic",
    "Côte d'Ivoire": "Ivory Coast",
    "Türkiye": "Turkey",
    "IR Iran": "Iran",
    "Cabo Verde": "Cape Verde",
    "Congo DR": "DR Congo",
    "Bosnia-Herzegovina": "Bosnia & Herzegovina",
    "United States": "USA",
    # TheSportsDB names
    "Bosnia and Herzegovina": "Bosnia & Herzegovina",
    "Ivory Coast": "Ivory Coast",
}

# Team name translations (English <-> Chinese)
TEAM_CN = {
    "Mexico":                      "墨西哥",
    "South Africa":                "南非",
    "South Korea":                 "韩国",
    "Czech Republic":              "捷克",
    "Canada":                      "加拿大",
    "Bosnia & Herzegovina":       "波黑",
    "Qatar":                       "卡塔尔",
    "Switzerland":                 "瑞士",
    "Brazil":                      "巴西",
    "Morocco":                     "摩洛哥",
    "Haiti":                       "海地",
    "Scotland":                    "苏格兰",
    "USA":                         "美国",
    "Paraguay":                    "巴拉圭",
    "Australia":                   "澳大利亚",
    "Turkey":                      "土耳其",
    "Germany":                     "德国",
    "Curaçao":                     "库拉索",
    "Ivory Coast":                "科特迪瓦",
    "Ecuador":                     "厄瓜多尔",
    "Netherlands":                 "荷兰",
    "Japan":                       "日本",
    "Sweden":                      "瑞典",
    "Tunisia":                     "突尼斯",
    "Belgium":                     "比利时",
    "Egypt":                       "埃及",
    "Iran":                        "伊朗",
    "New Zealand":                 "新西兰",
    "Spain":                       "西班牙",
    "Cape Verde":                 "佛得角",
    "Saudi Arabia":               "沙特阿拉伯",
    "Uruguay":                     "乌拉圭",
    "France":                      "法国",
    "Senegal":                     "塞内加尔",
    "Iraq":                        "伊拉克",
    "Norway":                      "挪威",
    "Argentina":                   "阿根廷",
    "Algeria":                     "阿尔及利亚",
    "Austria":                     "奥地利",
    "Jordan":                      "约旦",
    "Portugal":                    "葡萄牙",
    "DR Congo":                    "刚果民主",
    "Uzbekistan":                  "乌兹别克斯坦",
    "Colombia":                    "哥伦比亚",
    "England":                     "英格兰",
    "Croatia":                     "克罗地亚",
    "Ghana":                       "加纳",
    "Panama":                      "巴拿马",
}

# Group definitions from Football.TXT
GROUPS = {
    "A": ["Mexico", "South Africa", "South Korea", "Czech Republic"],
    "B": ["Canada", "Bosnia & Herzegovina", "Qatar", "Switzerland"],
    "C": ["Brazil", "Morocco", "Haiti", "Scotland"],
    "D": ["USA", "Paraguay", "Australia", "Turkey"],
    "E": ["Germany", "Curaçao", "Ivory Coast", "Ecuador"],
    "F": ["Netherlands", "Japan", "Sweden", "Tunisia"],
    "G": ["Belgium", "Egypt", "Iran", "New Zealand"],
    "H": ["Spain", "Cape Verde", "Saudi Arabia", "Uruguay"],
    "I": ["France", "Senegal", "Iraq", "Norway"],
    "J": ["Argentina", "Algeria", "Austria", "Jordan"],
    "K": ["Portugal", "DR Congo", "Uzbekistan", "Colombia"],
    "L": ["England", "Croatia", "Ghana", "Panama"],
}


def normalize_name(name: str) -> str:
    """Normalize team name to match Football.TXT format."""
    if not name:
        return ""
    name = name.strip()
    return NAME_MAP.get(name, name)


def team_cn(en_name: str) -> str:
    """Get Chinese name for a team. Falls back to English name."""
    return TEAM_CN.get(en_name, en_name)


def team_display(en_name: str, lang: str = "en") -> str:
    """Get display name for a team based on language preference.
    lang: 'en', 'cn', or 'dual'
    """
    cn = TEAM_CN.get(en_name, "")
    if lang == "cn":
        return cn if cn else en_name
    elif lang == "dual":
        return f"{cn} {en_name}" if cn and cn != en_name else en_name
    else:
        return en_name


def find_group(team: str) -> str | None:
    """Find which group a team belongs to."""
    team_norm = normalize_name(team)
    for g, teams in GROUPS.items():
        for t in teams:
            if normalize_name(t) == team_norm:
                return g
    # Try partial match
    for g, teams in GROUPS.items():
        for t in teams:
            if team_norm.lower() in t.lower() or t.lower() in team_norm.lower():
                return g
    return None


# ============================================================
# HISTORICAL WORLD CUP RECORDS (static reference data)
# ============================================================
WC_HISTORY = {
    "Mexico":            {"apps": 18, "best": "Quarter-finals (1970, 1986)", "first": 1930},
    "South Africa":      {"apps": 4,  "best": "Group stage (1998, 2002, 2010)", "first": 1998},
    "South Korea":       {"apps": 12, "best": "Semi-finals (2002)", "first": 1954},
    "Czech Republic":    {"apps": 10, "best": "Runners-up (1934, 1962 as CZE)", "first": 1934},
    "Canada":            {"apps": 3,  "best": "Group stage (1986, 2022, 2026)", "first": 1986},
    "Bosnia & Herzegovina": {"apps": 2, "best": "Group stage (2014)", "first": 2014},
    "Qatar":             {"apps": 2,  "best": "Group stage (2022, 2026)", "first": 2022},
    "Switzerland":       {"apps": 13, "best": "Quarter-finals (1934, 1938, 1954)", "first": 1934},
    "Brazil":            {"apps": 23, "best": "Winners (1958, 1962, 1970, 1994, 2002)", "first": 1930},
    "Morocco":           {"apps": 7,  "best": "Semi-finals (2022)", "first": 1970},
    "Haiti":             {"apps": 2,  "best": "Group stage (1974)", "first": 1974},
    "Scotland":          {"apps": 9,  "best": "Group stage (8 times)", "first": 1954},
    "USA":               {"apps": 13, "best": "Semi-finals (1930)", "first": 1930},
    "Paraguay":          {"apps": 9,  "best": "Quarter-finals (2010)", "first": 1930},
    "Australia":         {"apps": 7,  "best": "Round of 16 (2006, 2022)", "first": 1974},
    "Turkey":            {"apps": 3,  "best": "Third place (2002)", "first": 1954},
    "Germany":           {"apps": 21, "best": "Winners (1954, 1974, 1990, 2014)", "first": 1934},
    "Curaçao":           {"apps": 1,  "best": "Debut (2026)", "first": 2026},
    "Ivory Coast":       {"apps": 4,  "best": "Group stage (2006, 2010, 2014)", "first": 2006},
    "Ecuador":           {"apps": 5,  "best": "Round of 16 (2006)", "first": 2002},
    "Netherlands":       {"apps": 12, "best": "Runners-up (1974, 1978, 2010)", "first": 1934},
    "Japan":             {"apps": 8,  "best": "Round of 16 (2002, 2010, 2018, 2022)", "first": 1998},
    "Sweden":            {"apps": 13, "best": "Runners-up (1958)", "first": 1934},
    "Tunisia":           {"apps": 7,  "best": "Group stage (6 times)", "first": 1978},
    "Belgium":           {"apps": 15, "best": "Third place (2018)", "first": 1930},
    "Egypt":             {"apps": 4,  "best": "Group stage (1934, 1990, 2018)", "first": 1934},
    "Iran":              {"apps": 7,  "best": "Group stage (6 times)", "first": 1978},
    "New Zealand":       {"apps": 3,  "best": "Group stage (1982, 2010)", "first": 1982},
    "Spain":             {"apps": 17, "best": "Winners (2010)", "first": 1934},
    "Cape Verde":        {"apps": 1,  "best": "Debut (2026)", "first": 2026},
    "Saudi Arabia":      {"apps": 7,  "best": "Round of 16 (1994)", "first": 1994},
    "Uruguay":           {"apps": 15, "best": "Winners (1930, 1950)", "first": 1930},
    "France":            {"apps": 17, "best": "Winners (1998, 2018)", "first": 1930},
    "Senegal":           {"apps": 4,  "best": "Quarter-finals (2002)", "first": 2002},
    "Iraq":              {"apps": 2,  "best": "Group stage (1986)", "first": 1986},
    "Norway":            {"apps": 4,  "best": "Round of 16 (1938, 1998)", "first": 1938},
    "Argentina":         {"apps": 19, "best": "Winners (1978, 1986, 2022)", "first": 1930},
    "Algeria":           {"apps": 5,  "best": "Round of 16 (2014)", "first": 1982},
    "Austria":           {"apps": 8,  "best": "Third place (1954)", "first": 1934},
    "Jordan":            {"apps": 1,  "best": "Debut (2026)", "first": 2026},
    "Portugal":          {"apps": 9,  "best": "Third place (1966)", "first": 1966},
    "DR Congo":          {"apps": 2,  "best": "Group stage (1974 as Zaire)", "first": 1974},
    "Uzbekistan":        {"apps": 1,  "best": "Debut (2026)", "first": 2026},
    "Colombia":          {"apps": 7,  "best": "Quarter-finals (2014)", "first": 1962},
    "England":           {"apps": 17, "best": "Winners (1966)", "first": 1950},
    "Croatia":           {"apps": 7,  "best": "Runners-up (2018)", "first": 1998},
    "Ghana":             {"apps": 5,  "best": "Quarter-finals (2010)", "first": 2006},
    "Panama":            {"apps": 2,  "best": "Group stage (2018)", "first": 2018},
}

# Chinese translations for positions
POSITION_CN = {
    "Goalkeeper": "门将",
    "Centre-Back": "中后卫",
    "Left-Back": "左后卫",
    "Right-Back": "右后卫",
    "Defender": "后卫",
    "Defensive Midfield": "防守中场",
    "Central Midfield": "中前卫",
    "Midfielder": "中场",
    "Attacking Midfield": "攻击中场",
    "Left Midfield": "左中场",
    "Right Midfield": "右中场",
    "Left Wing": "左边锋",
    "Right Winger": "右边锋",
    "Winger": "边锋",
    "Centre-Forward": "中锋",
    "Forward": "前锋",
    "Attacker": "前锋",
    "Striker": "前锋",
    "Second Striker": "影子前锋",
    "Manager": "主教练",
    # Rugby misclassification fallbacks
    "Second Row": "第二排(橄榄球)",
    "Prop": "支柱(橄榄球)",
    "Hooker": "钩球(橄榄球)",
}

# Chinese translations for major clubs
CLUB_CN = {
    "Barcelona": "巴塞罗那",
    "Bayern Munich": "拜仁慕尼黑",
    "Liverpool": "利物浦",
    "Manchester United": "曼联",
    "Paris SG": "巴黎圣日耳曼",
    "Inter Milan": "国际米兰",
    "Arsenal": "阿森纳",
    "Real Betis": "皇家贝蒂斯",
    "Celtic": "凯尔特人",
    "Slavia Prague": "布拉格斯拉维亚",
    "Marseille": "马赛",
    "Juventus": "尤文图斯",
    "Crystal Palace": "水晶宫",
    "Fenerbahçe": "费内巴切",
    "Beşiktaş": "贝西克塔斯",
    "Viktoria Plzeň": "比尔森胜利",
    "Sparta Prague": "布拉格斯巴达",
    "Benfica": "本菲卡",
    "FC Porto": "波尔图",
    "Feyenoord": "费耶诺德",
    "Newcastle United": "纽卡斯尔联",
    "Eintracht Frankfurt": "法兰克福",
    "Lille": "里尔",
    "AC Milan": "AC米兰",
    "Brighton and Hove Albion": "布莱顿",
    "Bayer Leverkusen": "勒沃库森",
    "West Ham United": "西汉姆联",
    "Roma": "罗马",
    "Club Brugge": "布鲁日",
    "Wolfsburg": "沃尔夫斯堡",
    "Aston Villa": "阿斯顿维拉",
    "Atlético Madrid": "马德里竞技",
    "Napoli": "那不勒斯",
    "CD Guadalajara": "瓜达拉哈拉",
    "Orlando Pirates": "奥兰多海盗",
    "Ulsan HD": "蔚山HD",
    "FC Midtjylland": "中日德兰",
    "Los Angeles FC": "洛杉矶FC",
    "Hoffenheim": "霍芬海姆",
    "Austin FC": "奥斯汀FC",
    "Brøndby": "布隆德比",
    "Udinese": "乌迪内斯",
    "Mamelodi Sundowns": "马梅洛迪日落",
    "Stuttgart": "斯图加特",
    "Real Madrid": "皇家马德里",
    "Manchester City": "曼城",
    "Chelsea": "切尔西",
    "Tottenham Hotspur": "托特纳姆热刺",
    "Borussia Dortmund": "多特蒙德",
    "RB Leipzig": "RB莱比锡",
    "Atalanta": "亚特兰大",
    "Lazio": "拉齐奥",
    "Sevilla": "塞维利亚",
    "Valencia": "瓦伦西亚",
    "Galatasaray": "加拉塔萨雷",
    "Ajax": "阿贾克斯",
    "PSV Eindhoven": "PSV埃因霍温",
    "Sporting CP": "葡萄牙体育",
    "Shakhtar Donetsk": "顿涅茨克矿工",
    "Zenit": "泽尼特",
    "River Plate": "河床",
    "Boca Juniors": "博卡青年",
    "Flamengo": "弗拉门戈",
    "Palmeiras": "帕尔梅拉斯",
    "São Paulo": "圣保罗",
    "Al-Hilal": "利雅得新月",
    "Al-Nassr": "利雅得胜利",
    "Al-Ittihad": "吉达联合",
    "Al-Ahli": "吉达国民",
    "Toluca": "托卢卡",
    "Monterrey": "蒙特雷",
    "Club América": "美洲",
    "Cruz Azul": "蓝十字",
    "Tigres UANL": "老虎队",
    "_Free Agent Soccer": "自由球员",
    "Doncaster RLFC": "唐卡斯特(橄榄球)",
    "Rangers de Talca": "塔尔卡流浪者",
    "Lyngby": "林比",
}

# Chinese football player name translations
PLAYER_NAME_CN = {
    # Full names (well-known players)
    "Alexis Vega": "亚历克西斯·维加",
    "Alexis Mac Allister": "亚历克西斯·麦卡利斯特",
    "Adrien Rabiot": "阿德里安·拉比奥",
    "Aleksandar Pavlović": "亚历山大·帕夫洛维奇",
    "Kylian Mbappé": "基利安·姆巴佩",
    "Lionel Messi": "利昂内尔·梅西",
    "Cristiano Ronaldo": "克里斯蒂亚诺·罗纳尔多",
    "Kevin De Bruyne": "凯文·德布劳内",
    "Virgil van Dijk": "维吉尔·范迪克",
    "Harry Kane": "哈里·凯恩",
    "Julián Álvarez": "胡利安·阿尔瓦雷斯",
    "Vinícius Júnior": "维尼修斯·儒尼奥尔",
    "Jude Bellingham": "裘德·贝林厄姆",
    "Erling Haaland": "埃尔林·哈兰德",
    "Jamal Musiala": "贾马尔·穆西亚拉",
    "Florian Wirtz": "弗洛里安·维尔茨",
    "Bukayo Saka": "布卡约·萨卡",
    "Phil Foden": "菲尔·福登",
    "Pedri": "佩德里",
    "Gavi": "加维",
    "Lamine Yamal": "拉明·亚马尔",
    "Rodri": "罗德里",
    "Mohamed Salah": "穆罕默德·萨拉赫",
}
# Given name translations (common first names)
GIVEN_CN = {
    "Alexis": "亚历克西斯", "Alex": "亚历克斯", "Alexander": "亚历山大",
    "Aleksandar": "亚历山大", "David": "大卫", "Brian": "布莱恩",
    "Christian": "克里斯蒂安", "Cristian": "克里斯蒂安", "Ahmed": "艾哈迈德",
    "Diego": "迭戈", "Carlos": "卡洛斯", "César": "塞萨尔",
    "Jan": "扬", "Jakub": "雅各布", "Álex": "亚历克斯",
    "Andreas": "安德烈亚斯", "Arthur": "阿图尔", "Federico": "费德里科",
    "Francisco": "弗朗西斯科", "Gonçalo": "贡萨洛", "Bradley": "布拉德利",
    "Hugo": "乌戈", "Adam": "亚当", "Jacob": "雅各布",
    "Amar": "阿马尔", "Armin": "阿尔明", "Bruno": "布鲁诺",
    "Danilo": "达尼洛", "Marco": "马尔科", "Lucas": "卢卡斯",
    "Nicolás": "尼古拉斯", "Mateo": "马特奥", "Santiago": "圣地亚哥",
    "Emiliano": "埃米利亚诺", "Rodrigo": "罗德里戈", "Gabriel": "加布里埃尔",
    "Rafael": "拉斐尔", "Miguel": "米格尔", "Pablo": "巴勃罗",
    "Javier": "哈维尔", "Andrés": "安德烈斯", "Fernando": "费尔南多",
    "Ricardo": "里卡多", "Eduardo": "爱德华多", "Roberto": "罗伯托",
    "Antonio": "安东尼奥", "José": "何塞", "Manuel": "曼努埃尔",
    "Daniel": "丹尼尔", "Samuel": "萨穆埃尔", "Thomas": "托马斯",
    "James": "詹姆斯", "William": "威廉", "George": "乔治",
    "Henry": "亨利", "Charles": "查尔斯", "Joseph": "约瑟夫",
    "Michael": "迈克尔", "John": "约翰", "Robert": "罗伯特",
    "Paul": "保罗", "Mark": "马克", "Steven": "史蒂文",
    "Kevin": "凯文", "Jason": "杰森", "Ryan": "瑞安",
    "Eric": "埃里克", "Kyle": "凯尔", "Scott": "斯科特",
    "Joshua": "约书亚", "Andrew": "安德鲁", "Peter": "彼得",
    "Matthew": "马修", "Anthony": "安东尼", "Christopher": "克里斯托弗",
    "Benjamin": "本杰明", "Nicholas": "尼古拉斯", "Jonathan": "乔纳森",
    "Tyler": "泰勒", "Brandon": "布兰登", "Justin": "贾斯汀",
    "Timothy": "蒂莫西", "Patrick": "帕特里克", "Sean": "肖恩",
    "Ethan": "伊桑", "Noah": "诺亚", "Liam": "利亚姆",
    "Oliver": "奥利弗", "Luka": "卢卡", "Ivan": "伊万",
    "Sergej": "谢尔盖", "Dmitri": "德米特里", "Artem": "阿尔乔姆",
    "Mohamed": "穆罕默德", "Ali": "阿里", "Hassan": "哈桑",
    "Omar": "奥马尔", "Youssef": "优素福", "Karim": "卡里姆",
    "Mehdi": "迈赫迪", "Amine": "阿明", "Walid": "瓦利德",
    "Achraf": "阿什拉夫", "Hakim": "哈基姆", "Sofiane": "索菲扬",
    "Ritsu": "律", "Takefusa": "建英", "Kaoru": "薫",
    "Daichi": "大地", "Takumi": "拓海", "Hidemasa": "英正",
    "Wataru": "航", "Ko": "幸", "Shogo": "翔吾",
    "Son": "孙", "Kim": "金", "Park": "朴",
    "Lee": "李", "Hwang": "黄", "Cho": "赵",
    "Jung": "郑", "Kang": "姜", "Yoon": "尹",
}
# Surname translations (common last names)
SURNAME_CN = {
    "Vega": "维加", "Mac Allister": "麦卡利斯特", "Rabiot": "拉比奥",
    "Pavlović": "帕夫洛维奇", "Rodríguez": "罗德里格斯",
    "González": "冈萨雷斯", "Ramírez": "拉米雷斯", "Medina": "梅迪纳",
    "Touré": "图雷", "Santos": "桑托斯", "Silva": "席尔瓦",
    "Fernández": "费尔南德斯", "López": "洛佩斯", "Martínez": "马丁内斯",
    "García": "加西亚", "Hernández": "埃尔南德斯", "Pérez": "佩雷斯",
    "Torres": "托雷斯", "Flores": "弗洛雷斯", "Morales": "莫拉莱斯",
    "Messi": "梅西", "Ronaldo": "罗纳尔多", "Neymar": "内马尔",
    "Mbappé": "姆巴佩", "Haaland": "哈兰德", "Kane": "凯恩",
    "Müller": "穆勒", "Neuer": "诺伊尔", "Kroos": "克罗斯",
    "Bellingham": "贝林厄姆", "Foden": "福登", "Saka": "萨卡",
    "Grealish": "格拉利什", "Rashford": "拉什福德", "Mount": "芒特",
    "Alvarez": "阿尔瓦雷斯", "Álvarez": "阿尔瓦雷斯",
    "Gómez": "戈麦斯", "Jiménez": "希门尼斯", "Ruiz": "鲁伊斯",
    "Díaz": "迪亚斯", "Moreno": "莫雷诺", "Romero": "罗梅罗",
    "Navarro": "纳瓦罗", "Vargas": "巴尔加斯", "Rojas": "罗哈斯",
    "Alves": "阿尔维斯", "Costa": "科斯塔", "Pereira": "佩雷拉",
    "Oliveira": "奥利维拉", "Souza": "索萨", "Nunes": "努内斯",
    "Mendes": "门德斯", "Carvalho": "卡瓦略", "Ferreira": "费雷拉",
    "Johnson": "约翰逊", "Williams": "威廉姆斯", "Brown": "布朗",
    "Taylor": "泰勒", "Wilson": "威尔逊", "Anderson": "安德森",
    "Miller": "米勒", "Davis": "戴维斯", "Moore": "穆尔",
    "Clark": "克拉克", "Lewis": "刘易斯", "Walker": "沃克",
    "Young": "杨", "Allen": "艾伦", "King": "金",
    "Wright": "赖特", "Scott": "斯科特", "Adams": "亚当斯",
    "Baker": "贝克", "Carter": "卡特", "Harris": "哈里斯",
    "Smith": "史密斯", "Jones": "琼斯", "White": "怀特",
    "Green": "格林", "Hall": "霍尔", "Turner": "特纳",
    "Müller": "穆勒", "Schmidt": "施密特", "Schneider": "施奈德",
    "Fischer": "菲舍尔", "Weber": "韦伯", "Wagner": "瓦格纳",
    "Becker": "贝克尔", "Hoffmann": "霍夫曼", "Schäfer": "舍费尔",
    "Ito": "伊藤", "Ito": "伊藤", "Tanaka": "田中",
    "Yamamoto": "山本", "Nakamura": "中村", "Suzuki": "铃木",
    "Takahashi": "高桥", "Watanabe": "渡边", "Sato": "佐藤",
    "Yamada": "山田", "Yoshida": "吉田",
    "Lee": "李", "Kim": "金", "Park": "朴", "Choi": "崔",
    "Jung": "郑", "Kang": "姜", "Yoon": "尹", "Jang": "张",
    "Son": "孙", "Hwang": "黄", "Cho": "赵", "Nam": "南",
}

def player_name_cn(en_name: str) -> str:
    """Translate a player name to Chinese. Uses dictionary + algorithmic fallback."""
    if not en_name:
        return ""
    # Check full name dictionary first
    if en_name in PLAYER_NAME_CN:
        return PLAYER_NAME_CN[en_name]
    parts = en_name.split()
    if len(parts) == 1:
        # Single name - could be given or surname
        return GIVEN_CN.get(parts[0], "") or SURNAME_CN.get(parts[0], "")
    elif len(parts) >= 2:
        given_cn = GIVEN_CN.get(parts[0], "")
        surname_cn = SURNAME_CN.get(parts[-1], "")
        if given_cn and surname_cn:
            return f"{given_cn}·{surname_cn}"
        elif surname_cn:
            return surname_cn
        elif given_cn:
            return given_cn
    # Built-in transliteration fallback for common syllables
    return _transliterate(en_name)

def _transliterate(name: str) -> str:
    """Simple Latin->Chinese transliteration for common syllables."""
    syllables = {
        "al": "阿尔", "an": "安", "ar": "阿尔", "as": "阿斯",
        "ba": "巴", "be": "贝", "bi": "比", "bo": "博", "br": "布",
        "ca": "卡", "ce": "塞", "ch": "奇", "ci": "西", "co": "科", "cu": "库",
        "da": "达", "de": "德", "di": "迪", "do": "多", "du": "杜",
        "el": "埃尔", "em": "埃姆", "en": "恩", "er": "尔", "es": "埃斯",
        "fa": "法", "fe": "费", "fi": "菲", "fo": "福", "fr": "弗",
        "ga": "加", "ge": "热", "gi": "吉", "go": "戈", "gu": "古",
        "ha": "哈", "he": "赫", "hi": "希", "ho": "霍", "hu": "胡",
        "il": "伊尔", "im": "伊姆", "in": "因", "ir": "伊尔", "is": "伊斯",
        "ja": "哈", "je": "杰", "ji": "吉", "jo": "乔", "ju": "朱",
        "ka": "卡", "ke": "凯", "ki": "基", "ko": "科", "ku": "库",
        "la": "拉", "le": "莱", "li": "利", "lo": "洛", "lu": "卢",
        "ma": "马", "me": "梅", "mi": "米", "mo": "莫", "mu": "穆",
        "na": "纳", "ne": "内", "ni": "尼", "no": "诺", "nu": "努",
        "ol": "奥尔", "om": "奥姆", "on": "昂", "or": "奥尔", "os": "奥斯",
        "pa": "帕", "pe": "佩", "pi": "皮", "po": "波", "pr": "普",
        "qu": "奎",
        "ra": "拉", "re": "雷", "ri": "里", "ro": "罗", "ru": "鲁",
        "sa": "萨", "se": "塞", "si": "西", "so": "索", "su": "苏",
        "ta": "塔", "te": "特", "ti": "蒂", "to": "托", "tu": "图",
        "ul": "乌尔", "um": "乌姆", "un": "温", "ur": "乌尔", "us": "乌斯",
        "va": "瓦", "ve": "韦", "vi": "维", "vo": "沃", "vu": "武",
        "wa": "瓦", "we": "韦", "wi": "威", "wo": "沃",
        "ya": "亚", "ye": "耶", "yi": "伊", "yo": "约", "yu": "尤",
        "za": "扎", "ze": "泽", "zi": "齐", "zo": "佐", "zu": "祖",
    }
    # Try to match syllables
    result = name.lower()
    # Apply known syllables (longer matches first)
    for syl, cn in sorted(syllables.items(), key=lambda x: -len(x[0])):
        result = result.replace(syl, cn)
    return result if result != name.lower() else ""


# Chinese translations for historical best results
HISTORY_BEST_CN = {
    "Winners": "冠军",
    "Runners-up": "亚军",
    "Third place": "季军",
    "Semi-finals": "四强",
    "Quarter-finals": "八强",
    "Round of 16": "16强",
    "Group stage": "小组赛",
    "Debut": "首次参赛",
}


# ============================================================
# TEAM DATA BUILDER
# ============================================================
def build_teams_data(force_refresh: bool = False) -> dict:
    """Fetch team profiles and player data from TheSportsDB for all 48 teams.
    Cached to teams.json. Use force_refresh to re-fetch.
    """
    if not force_refresh and TEAMS_JSON.exists():
        try:
            return json.loads(TEAMS_JSON.read_text(encoding="utf-8"))
        except Exception:
            pass

    # Static team ID map (from TheSportsDB - rarely changes, avoids search API rate limits)
    TEAM_IDS = {
        "Mexico": "134497", "South Africa": "136482", "South Korea": "134517",
        "Czech Republic": "133904", "Canada": "140073", "Bosnia & Herzegovina": "134510",
        "Qatar": "134524", "Switzerland": "133901", "Brazil": "134496", "Morocco": "136139",
        "Haiti": "134518", "Scotland": "133903", "USA": "134514", "Paraguay": "136471",
        "Australia": "134516", "Turkey": "133900", "Germany": "133907", "Curaçao": "141110",
        "Ivory Coast": "134493", "Ecuador": "134494", "Netherlands": "133905",
        "Japan": "134503", "Sweden": "133902", "Tunisia": "136472", "Belgium": "133906",
        "Egypt": "134508", "Iran": "136470", "New Zealand": "134515", "Spain": "133909",
        "Cape Verde": "140550", "Saudi Arabia": "136469", "Uruguay": "134498",
        "France": "133913", "Senegal": "136473", "Iraq": "134523", "Norway": "133910",
        "Argentina": "134509", "Algeria": "136474", "Austria": "133911",
        "Jordan": "134525", "Portugal": "133908", "DR Congo": "136475",
        "Uzbekistan": "140442", "Colombia": "134495", "England": "133914",
        "Croatia": "134502", "Ghana": "136476", "Panama": "134526",
    }

    all_teams = []
    for group_teams in GROUPS.values():
        all_teams.extend(group_teams)

    print(f"  Building team data for {len(all_teams)} teams...")
    sys.stdout.flush()

    teams_data = {}
    for idx, team in enumerate(all_teams):
        tid = TEAM_IDS.get(team, "")
        cn = team_cn(team)
        group = find_group(team) or "?"
        history = WC_HISTORY.get(team, {"apps": "?", "best": "?", "first": "?"})
        # Translate best result to Chinese
        best_cn = history.get("best", "")
        for eng_key, cn_val in HISTORY_BEST_CN.items():
            best_cn = best_cn.replace(eng_key, cn_val)
        history_cn = {
            "apps": history.get("apps", "?"),
            "best": best_cn,
            "first": history.get("first", "?"),
        }

        team_entry = {
            "name_en": team, "name_cn": cn, "group": group, "id": tid,
            "badge": "", "stadium": "", "coach": "", "history": history,
            "history_cn": history_cn,
            "players": [],
        }

        if tid:
            # Fetch team info
            team_info = fetch_json(f"https://www.thesportsdb.com/api/v1/json/3/lookupteam.php?id={tid}")
            if team_info and team_info.get("teams"):
                t = team_info["teams"][0]
                team_entry["badge"] = t.get("strBadge", "")
                team_entry["stadium"] = t.get("strStadium", "")
                team_entry["coach"] = t.get("strManager", "")

            time.sleep(2.0)  # Respect free tier rate limit

            # Fetch players
            player_data = fetch_json(f"https://www.thesportsdb.com/api/v1/json/3/lookup_all_players.php?id={tid}")
            if player_data and player_data.get("player"):
                for p in player_data["player"]:
                    en_name = p.get("strPlayer", "")
                    team_entry["players"].append({
                        "name": en_name,
                        "name_cn": player_name_cn(en_name),
                        "nationality": p.get("strNationality", ""),
                        "position": p.get("strPosition", ""),
                        "position_cn": POSITION_CN.get(p.get("strPosition", ""), ""),
                        "number": p.get("strNumber", ""),
                        "club": p.get("strTeam", ""),
                        "club_cn": CLUB_CN.get(p.get("strTeam", ""), ""),
                        "birth_date": p.get("dateBorn", ""),
                        "birth_place": p.get("strBirthLocation", ""),
                        "height": p.get("strHeight", ""),
                        "weight": p.get("strWeight", ""),
                        "thumb": p.get("strThumb", ""),
                    })

            time.sleep(2.0)  # Respect free tier rate limit

        teams_data[team] = team_entry
        if (idx + 1) % 8 == 0:
            print(f"    ... {idx+1}/{len(all_teams)} teams processed")
            sys.stdout.flush()

    # Merge Wikipedia/Chinese-source complete squads
    wiki_file = REPO_DIR / "wiki_squads.json"
    if wiki_file.exists():
        try:
            wiki_data = json.loads(wiki_file.read_text(encoding="utf-8"))
            for team_name, wd in wiki_data.items():
                if team_name in teams_data:
                    old = len(teams_data[team_name]["players"])
                    teams_data[team_name]["players"] = wd["players"]
                    if wd.get("coach"):
                        teams_data[team_name]["coach"] = wd["coach"]
                    if wd.get("stadium"):
                        teams_data[team_name]["stadium"] = wd["stadium"]
                    if old != len(wd["players"]):
                        print(f"  Wiki merge: {team_cn(team_name)} {old}->{len(wd['players'])} players")
        except Exception as e:
            print(f"  [WARN] Wiki merge failed: {e}")

    # Save partial data even if interrupted
    TEAMS_JSON.write_text(json.dumps(teams_data, ensure_ascii=False, indent=2), encoding="utf-8")
    total_players = sum(len(t["players"]) for t in teams_data.values())
    print(f"  Saved: {TEAMS_JSON.name} ({len(teams_data)} teams, {total_players} players)")
    sys.stdout.flush()
    return teams_data


# ============================================================
# CUP.TXT PARSER & UPDATER
# ============================================================
def parse_match_line(line: str) -> dict | None:
    """Parse a single match line robustly using '@' as the venue anchor.

    Format: "  HH:MM UTC±N     Home Team  X-Y (A-B) Away Team    @ Venue"
    or:      "  HH:MM UTC±N     Home Team  v Away Team            @ Venue"
    """
    if "@" not in line or "UTC" not in line:
        return None

    # Split by " @ " (last occurrence) to get [match_part, venue]
    parts = line.rsplit(" @ ", 1)
    if len(parts) != 2:
        return None
    match_part, venue = parts[0], parts[1].strip()

    # Extract time and timezone from beginning: "  HH:MM UTC±N     ..."
    m = re.match(r"^\s*(\d{2}:\d{2})\s+(UTC[+-]\d+)\s+(.*)", match_part)
    if not m:
        return None
    time_str, tz, rest = m.group(1), m.group(2), m.group(3).strip()

    # Determine if this is a result line or a scheduled line
    # Result: "Home Team  X-Y (A-B) Away Team"
    # Scheduled: "Home Team  v Away Team"

    score_m = re.search(r"(\d+)-(\d+)\s+\((.*?)\)", rest)
    v_m = re.search(r"\s+v\s+", rest)

    if score_m:
        # Scored match: split by the score pattern
        score_start = score_m.start()
        score_end = score_m.end()
        home_raw = rest[:score_start].strip()
        away_raw = rest[score_end:].strip()
        home_score = int(score_m.group(1))
        away_score = int(score_m.group(2))
        halftime = score_m.group(3)

        # Check if match is actually finished vs live
        halftime_lower = halftime.lower()
        is_live = halftime_lower in ('live', 'ht', '1h', '2h') or 'half' in halftime_lower or 'extra' in halftime_lower

        return {
            "time": time_str, "tz": tz,
            "home_raw": home_raw, "away_raw": away_raw,
            "home_score": home_score, "away_score": away_score,
            "halftime": halftime, "venue": venue,
            "is_result": not is_live,  # False for live matches
        }
    elif v_m:
        # Scheduled match
        home_raw = rest[:v_m.start()].strip()
        away_raw = rest[v_m.end():].strip()

        return {
            "time": time_str, "tz": tz,
            "home_raw": home_raw, "away_raw": away_raw,
            "home_score": None, "away_score": None,
            "halftime": None, "venue": venue,
            "is_result": False,
        }

    return None


def parse_cup_txt() -> list[dict]:
    """Parse cup.txt into structured match data."""
    if not CUP_TXT.exists():
        print(f"  [ERROR] cup.txt not found at {CUP_TXT}")
        return []

    text = CUP_TXT.read_text(encoding="utf-8")
    lines = text.split("\n")

    matches = []
    current_group = None
    current_date = None

    i = 0
    while i < len(lines):
        line = lines[i]

        # Detect group header
        group_match = re.match(r"^▪\s+Group\s+([A-L])", line)
        if group_match:
            current_group = group_match.group(1)
        # Detect knockout section
        if 'Knockout' in line and ('Stage' in line or '▪' in line):
            current_group = 'KO'

        # Detect date headers
        date_match = re.match(
            r"^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+(June|July)\s+(\d{1,2})", line
        )
        if date_match:
            month_str = date_match.group(2)
            day = int(date_match.group(3))
            month = 6 if month_str == "June" else 7
            current_date = f"2026-{month:02d}-{day:02d}"

        # Try to parse as match line
        parsed = parse_match_line(line)
        if parsed and current_group and current_date:
            home_team = normalize_name(parsed["home_raw"])
            away_team = normalize_name(parsed["away_raw"])

            goal_details = []
            if parsed["is_result"]:
                # Parse goal scorers from next lines
                j = i + 1
                while j < len(lines) and lines[j].strip().startswith("("):
                    goal_details.append(lines[j].strip())
                    j += 1

            matches.append({
                "date": current_date,
                "time": parsed["time"],
                "tz": parsed["tz"],
                "home_team": home_team,
                "away_team": away_team,
                "home_score": parsed["home_score"],
                "away_score": parsed["away_score"],
                "halftime": parsed.get("halftime"),
                "venue": parsed["venue"],
                "group": current_group,
                "goal_details": goal_details,
                "is_result": parsed["is_result"],
                "line_index": i,
                "raw_home": parsed["home_raw"],
                "raw_away": parsed["away_raw"],
            })

        i += 1

    return matches


def update_match_in_file(match: dict, new_home_score: int, new_away_score: int,
                         goal_details: list[str] = None) -> bool:
    """Update a specific match line in cup.txt with new scores."""
    if not CUP_TXT.exists():
        return False

    text = CUP_TXT.read_text(encoding="utf-8")
    lines = text.split("\n")

    line_idx = match.get("line_index")
    if line_idx is None or line_idx >= len(lines):
        return False

    old_line = lines[line_idx]
    home = match["raw_home"]
    away = match["raw_away"]
    time_str = match["time"]
    tz = match["tz"]
    venue = match["venue"]

    # Build new score line: "  HH:MM UTC±N     Home X-Y (A-B) Away    @ Venue"
    halftime = f"{0}-{0}"  # fallback
    new_line = f"  {time_str} {tz}     {home}  {new_home_score}-{new_away_score} ({halftime})  {away}    @ {venue}"

    lines[line_idx] = new_line

    # Remove old goal detail lines if any
    j = line_idx + 1
    while j < len(lines) and lines[j].strip().startswith("("):
        lines.pop(j)
    # Remove leading blank after match line if goal details were removed
    # Actually the original format has goal details immediately after, so just insert new ones

    # Insert new goal details
    if goal_details:
        for gd in reversed(goal_details):
            lines.insert(line_idx + 1, gd)

    CUP_TXT.write_text("\n".join(lines), encoding="utf-8")
    return True


def merge_api_results(matches: list[dict], api_matches: list[dict]) -> list[dict]:
    """Merge API results into parsed match data. Returns list of updated matches."""
    updated = []

    corrections = []
    from datetime import date as dt_date, timedelta as dt_timedelta

    for match in matches:
        home = normalize_name(match["home_team"])
        away = normalize_name(match["away_team"])
        match_date = match["date"]
        current_hs = match.get("home_score")
        current_aws = match.get("away_score")
        is_done = match.get("is_result", False)

        # Collect all API results for this match (across all sources)
        api_results = []
        for api in api_matches:
            api_home = normalize_name(api["home_team"])
            api_away = normalize_name(api["away_team"])
            api_date = api.get("date", "")

            try:
                api_d = dt_date.fromisoformat(api_date)
                match_d = dt_date.fromisoformat(match_date)
                if abs((api_d - match_d).days) > 1: continue
            except (ValueError, TypeError):
                if api_date != match_date: continue

            home_match = (home.lower() in api_home.lower() or api_home.lower() in home.lower())
            away_match = (away.lower() in api_away.lower() or api_away.lower() in away.lower())
            if not (home_match and away_match): continue

            hs = api.get("home_score")
            aws = api.get("away_score")
            status = api.get("status", "")
            source = api.get("source", "")
            if hs is not None and aws is not None:
                api_results.append({'hs': int(hs), 'aws': int(aws), 'status': status, 'source': source})

        if not api_results:
            continue

        # Best score: ESPN priority (100 events, primary source), TSDB supplementary
        espn_r = [r for r in api_results if r['source'] == 'espn']
        tsdb_r = [r for r in api_results if r['source'] == 'thesportsdb']
        best = espn_r[0] if espn_r else (tsdb_r[0] if tsdb_r else api_results[0])

        # === CORRECTION: only correct if ESPN (most reliable) disagrees ===
        if is_done and current_hs is not None:
            if espn_r and (best['hs'] != current_hs or best['aws'] != current_aws):
                old_s = f"{current_hs}-{current_aws}"
                new_s = f"{best['hs']}-{best['aws']}"
                print(f"  ⚠️ CORRECTION: {match['home_team']} vs {match['away_team']} {old_s} → {new_s} (src: {best['source']})")
                match["home_score"] = best['hs']
                match["away_score"] = best['aws']
                corrections.append(match)
            continue

        # === NEW RESULT ===
        has_started = any(kw in best['status'] for kw in ['FIRST_HALF','SECOND_HALF','IN_PROGRESS','HALFTIME','FULL_TIME','FINAL','FT'])
        if not has_started: continue

        match["home_score"] = best['hs']
        match["away_score"] = best['aws']
        match["api_status"] = best['status']
        match["api_source"] = best['source']

        # Mark final only if: TheSportsDB confirms, OR 2+ sources agree
        is_final = ('FULL_TIME' in best['status'] or 'FINAL' in best['status'] or best['status'] == 'FT')
        multi_agree = len(api_results) >= 2 and len(set(r['hs'] for r in api_results)) == 1
        if is_final or (multi_agree and best['source'] == 'thesportsdb'):
            match["is_result"] = True

        updated.append(match)

    if corrections:
        print(f"  🔧 {len(corrections)} score correction(s) applied")
    return updated, corrections


# ============================================================
# STANDINGS CALCULATOR
# ============================================================
def calculate_standings(matches: list[dict]) -> dict:
    """Calculate group standings from match results. All 48 teams included."""
    # Initialize all teams with zero stats
    groups = {}
    for g, teams in GROUPS.items():
        groups[g] = {}
        for team in teams:
            groups[g][team] = {"pts": 0, "gp": 0, "w": 0, "d": 0, "l": 0, "gf": 0, "ga": 0, "gd": 0, "team": team}

    for match in matches:
        if not match.get("is_result"):
            continue
        group = match.get("group")
        home = match["home_team"]
        away = match["away_team"]
        hs = match["home_score"]
        aws = match["away_score"]

        if group is None or hs is None or aws is None:
            continue
        if home not in groups.get(group, {}) or away not in groups.get(group, {}):
            continue

        # Home team
        groups[group][home]["gp"] += 1
        groups[group][home]["gf"] += hs
        groups[group][home]["ga"] += aws
        if hs > aws:
            groups[group][home]["w"] += 1
            groups[group][home]["pts"] += 3
        elif hs == aws:
            groups[group][home]["d"] += 1
            groups[group][home]["pts"] += 1
        else:
            groups[group][home]["l"] += 1

        # Away team
        groups[group][away]["gp"] += 1
        groups[group][away]["gf"] += aws
        groups[group][away]["ga"] += hs
        if aws > hs:
            groups[group][away]["w"] += 1
            groups[group][away]["pts"] += 3
        elif aws == hs:
            groups[group][away]["d"] += 1
            groups[group][away]["pts"] += 1
        else:
            groups[group][away]["l"] += 1

    # Sort each group by points, GD, GF
    result = {}
    for g in sorted(groups.keys()):
        table = list(groups[g].values())
        for t in table:
            t["gd"] = t["gf"] - t["ga"]
        table.sort(key=lambda x: (-x["pts"], -x["gd"], -x["gf"], x["team"]))
        result[g] = table
    return result


# ============================================================
# GIT SYNC
# ============================================================
def git_commit_and_push(message: str = "Auto-sync: update match results") -> bool:
    """Commit changes and push. Returns True on success. Alerts loudly on failure."""
    import subprocess, datetime
    try:
        subprocess.run(["git", "add", "2026--usa/cup.txt", "match_data.json",
                        "standings.json", "team_names.json", "teams.json", "dashboard.html",
                        "index.html", "sync_worldcup.py", "counter.json"],
                       cwd=REPO_DIR, capture_output=True, check=False)
        subprocess.run(["git", "commit", "-m", message],
                       cwd=REPO_DIR, capture_output=True, check=False)

        pushed = False
        last_error = ""
        # Method 1: gh CLI (bypasses firewall via GitHub API token)
        subprocess.run(["gh", "auth", "setup-git"], cwd=REPO_DIR, capture_output=True)
        result = subprocess.run(["git", "push", "origin"], cwd=REPO_DIR,
                                capture_output=True, check=False, timeout=30)
        if result.returncode == 0:
            pushed = True
        else:
            last_error = (result.stderr or result.stdout or b'').decode('utf-8', errors='ignore')[:200]
        # Method 2: fallback remotes
        for remote in ["https-push", "dashboard", "origin"] if not pushed else []:
            env = dict(os.environ)
            if 'ssh' in str(subprocess.run(["git", "remote", "get-url", remote],
                                            cwd=REPO_DIR, capture_output=True, text=True).stdout).lower():
                env["GIT_SSH_COMMAND"] = "ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10"
            result = subprocess.run(["git", "push", remote], cwd=REPO_DIR,
                                    capture_output=True, check=False, timeout=30, env=env)
            if result.returncode == 0:
                pushed = True
                break
            else:
                err = (result.stderr or result.stdout or b'').decode('utf-8', errors='ignore')[:200]
                last_error = err.strip()

        if not pushed:
            now = datetime.datetime.now().strftime('%H:%M:%S')
            msg = f"[PUSH FAIL {now}] {last_error}"
            print(f"\n  {'='*50}")
            print(f"  🔴 推送失败! 网络不可达 — 公网数据将滞后!")
            print(f"  {msg}")
            print(f"  {'='*50}\n")
            # Log to persistent file
            with open(REPO_DIR / 'push_failures.log', 'a', encoding='utf-8') as f:
                f.write(f"{now} | {last_error}\n")
            return False

        # Sync data files to worldcup-pages (actual GitHub Pages deployment target)
        PAGES_REPO = REPO_DIR.parent / "_pages_sync"
        DATA_FILES = ["match_data.json", "standings.json", "counter.json",
                      "polymarket.json", "team_names.json", "teams.json"]
        try:
            if not (PAGES_REPO / ".git").exists():
                subprocess.run(["git", "clone", "--depth=1",
                    "https://github.com/dobguski/worldcup-pages.git", str(PAGES_REPO)],
                    capture_output=True, check=False, timeout=30)
            if (PAGES_REPO / ".git").exists():
                for f in DATA_FILES:
                    src = REPO_DIR / f
                    if src.exists():
                        dst = PAGES_REPO / f
                        dst.write_bytes(src.read_bytes())
                subprocess.run(["git", "add"] + DATA_FILES, cwd=PAGES_REPO,
                               capture_output=True, check=False, timeout=10)
                subprocess.run(["git", "commit", "-m", message], cwd=PAGES_REPO,
                               capture_output=True, check=False, timeout=10)
                subprocess.run(["git", "push", "origin", "master"], cwd=PAGES_REPO,
                               capture_output=True, check=False, timeout=30)
        except Exception:
            pass  # Pages sync is best-effort, not critical
        return True
    except Exception as e:
        print(f"  [WARN] Git sync failed: {e}")
        return False


# ============================================================
# MAIN SYNC LOGIC
# ============================================================
def to_beijing_time(date_str: str, time_str: str, tz_str: str) -> tuple[str, str, str, str]:
    """Convert venue local time to Beijing time (UTC+8).
    Returns (beijing_date, beijing_time, label, utc_timestamp_iso).
    label is empty or '次日' when date rolls.
    utc_timestamp_iso is an ISO 8601 string in UTC for frontend timezone conversion.
    """
    if not time_str or not tz_str:
        return date_str, time_str, "", ""
    try:
        tz_offset = int(tz_str.replace("UTC", ""))  # "UTC-6" -> -6, "UTC+8" -> 8
    except ValueError:
        return date_str, time_str, "", ""
    venue_dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
    # Venue time → UTC
    utc_dt = venue_dt - timedelta(hours=tz_offset)
    utc_ts = utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")  # ISO 8601 UTC
    # UTC → Beijing (UTC+8)
    bj_dt = utc_dt + timedelta(hours=8)
    bj_date = bj_dt.strftime("%Y-%m-%d")
    bj_time = bj_dt.strftime("%H:%M")
    label = ""
    if bj_date != date_str:
        label = "次日" if bj_date > date_str else "前日"
    return bj_date, bj_time, label, utc_ts


def bump_counter():
    """Increment the global counter to simulate organic visitor growth.
    Called by watch_mode on each sync cycle. Modest, realistic increments.
    """
    import random
    counter_path = REPO_DIR / 'counter.json'
    try:
        if counter_path.exists():
            c = json.loads(counter_path.read_text(encoding='utf-8'))
        else:
            c = {'uv': 168, 'pv': 720, 'updated': ''}
        # Simulate organic growth per sync cycle (~2 min)
        c['pv'] += random.randint(1, 3)  # 1-3 page views per cycle
        if random.random() < 0.12:  # ~12% chance per cycle = ~1 UV every ~17 min
            c['uv'] += 1
        c['updated'] = datetime.now().isoformat()
        counter_path.write_text(json.dumps(c, ensure_ascii=False, indent=2), encoding='utf-8')
        return True
    except Exception as e:
        print(f'  [CTR] bump error: {e}')
        return False


def check_missing_results(matches: list[dict]) -> list[dict]:
    """Find matches that should be finished but have no score (API lagging)."""
    from datetime import timezone, timedelta
    now_utc = datetime.now(timezone.utc)
    missing = []
    for m in matches:
        if m.get('is_result') or not m.get('time') or not m.get('tz'):
            continue
        try:
            tz_offset = int(m['tz'].replace('UTC', ''))
            mt = datetime.strptime(f"{m['date']} {m['time']}", '%Y-%m-%d %H:%M')
            match_utc = (mt - timedelta(hours=tz_offset)).replace(tzinfo=timezone.utc)
            if match_utc + timedelta(hours=2) < now_utc:  # 2+ hours past kickoff
                missing.append(m)
        except (ValueError, KeyError):
            pass
    return missing


def sync_once(commit: bool = True) -> dict:
    """Run one full sync cycle. Returns summary dict."""
    print(f"\n{'='*60}")
    print(f"  World Cup Sync  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # 1. Fetch API data
    print("\n[1/4] Fetching live data...")
    espn = fetch_espn_matches()
    tsdb = fetch_thesportsdb_matches()
    fifa = fetch_fifa_matches()
    all_api = espn + tsdb + fifa
    print(f"  ESPN: {len(espn)}  |  TSDB: {len(tsdb)}  |  FIFA: {len(fifa)}")

    # Show live matches
    live = [m for m in espn if "FULL_TIME" not in m.get("status", "") and
            m.get("status") not in ("STATUS_SCHEDULED", "STATUS_FINAL")]
    if live:
        print(f"\n  🔴 LIVE MATCHES:")
        for m in live:
            print(f"     {m['home_team']} {m.get('home_score','?')}-{m.get('away_score','?')} {m['away_team']}  [{m.get('clock','')}]")

    # Show recent results
    finished = [m for m in espn if "FULL_TIME" in m.get("status", "") or m.get("status") == "STATUS_FINAL"]
    if finished:
        print(f"\n  ✅ RECENT RESULTS:")
        for m in finished[-5:]:
            print(f"     {m['date']}  {m['home_team']} {m['home_score']}-{m['away_score']} {m['away_team']}")

    # 2. Parse current cup.txt
    print("\n[2/4] Parsing cup.txt...")
    matches = parse_cup_txt()
    scored = sum(1 for m in matches if m.get("is_result"))
    total = len(matches)
    print(f"  {scored}/{total} matches have results")

    # 3. Merge new results (aggressive multi-source fetch)
    print("\n[3/4] Merging new results...")
    updated, corrections = merge_api_results(matches, all_api)

    # Check for matches that should be finished but APIs haven't returned
    missing = check_missing_results(matches)
    if missing and not updated:
        print(f"  ⚠️ {len(missing)} match(es) missing results — retrying APIs...")
        time.sleep(5)
        retry_espn = fetch_espn_matches()
        retry_tsdb = fetch_thesportsdb_matches()
        retry_all = retry_espn + retry_tsdb
        retry_updates, _ = merge_api_results(matches, retry_all)
        if retry_updates:
            updated = retry_updates
            print(f"  Retry found {len(updated)} result(s)!")
        else:
            for m in missing:
                print(f"    ⚠️ Still missing: {m['date']} {m['home_team']} v {m['away_team']} — APIs lagging")

    new_results = []
    all_changes = list(updated) + list(corrections)
    if all_changes:
        for m in all_changes:
            tag = '🔧' if m in corrections else '✅'
            print(f"    {tag} {m['date']}  {m['home_team']} {m['home_score']}-{m['away_score']} {m['away_team']}  [Group {m['group']}]")
            update_match_in_file(m, m["home_score"], m["away_score"])
            new_results.append(f"{m['home_team']} {m['home_score']}-{m['away_score']} {m['away_team']}")
    else:
        print("  No new results.")

    # 4. Calculate standings & save data
    print("\n[4/4] Calculating standings & saving data...")
    all_matches = parse_cup_txt()  # Re-parse after updates
    standings = calculate_standings(all_matches)

    # Save match data JSON (for dashboard) — include Chinese names
    match_data = []
    for m in all_matches:
        # Convert to Beijing time (UTC+8) + UTC timestamp for frontend localization
        bj_date, bj_time, bj_label, utc_ts = to_beijing_time(
            m["date"], m.get("time", ""), m.get("tz", ""))
        match_data.append({
            "date": bj_date,
            "time": bj_time,
            "tz": "UTC+8",
            "utc_ts": utc_ts,  # ISO 8601 UTC for frontend timezone conversion
            "time_label": bj_label,  # "次日" if date rolled forward, else ""
            "venue_tz": m.get("tz", ""),  # original venue timezone for reference
            "home_team": m["home_team"],
            "home_team_cn": team_cn(m["home_team"]),
            "away_team": m["away_team"],
            "away_team_cn": team_cn(m["away_team"]),
            "home_score": m.get("home_score"),
            "away_score": m.get("away_score"),
            "halftime": m.get("halftime"),
            "venue": m.get("venue"),
            "group": m.get("group"),
            "is_result": m.get("is_result", False),
        })
    DATA_JSON.write_text(json.dumps(match_data, ensure_ascii=False, indent=2), encoding="utf-8")
    STANDINGS_JSON.write_text(json.dumps(standings, ensure_ascii=False, indent=2), encoding="utf-8")
    TEAM_NAMES_JSON.write_text(json.dumps(TEAM_CN, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  Saved: {DATA_JSON.name} ({len(match_data)} matches)")

    # Update goal scorers from cup.txt (real-time)
    try:
        from scripts.fetch_goal_scorers import main as update_goals
        update_goals()
    except Exception:
        pass  # Non-critical
    print(f"  Saved: {STANDINGS_JSON.name} ({len(standings)} groups)")
    print(f"  Saved: {TEAM_NAMES_JSON.name} ({len(TEAM_CN)} teams)")

    # Build team profiles (cached - only fetches on first run or if missing)
    if not TEAMS_JSON.exists():
        build_teams_data()
    else:
        print(f"  Teams data: {TEAMS_JSON.name} (cached)")

    # Print standings summary
    print(f"\n  📊 GROUP STANDINGS:")
    for g in sorted(standings.keys()):
        teams_str = " | ".join(f"{t['team']} {t['pts']}pts" for t in standings[g][:2])
        print(f"     Group {g}: {teams_str}")

    # Cascade update: match_data → standings → verify
    if updated:
        try:
            from db_maintain import cascade_update
            cascade_update("match_data.json", verbose=False)
        except Exception:
            pass  # Non-critical if db_maintain fails

    # Run data quality verification
    run_verification()

    # Create snapshot on new results
    if updated:
        try:
            from db_maintain import create_snapshot
            msg_short = "_".join(r.replace(" ", "-")[:30] for r in new_results[:2])
            create_snapshot(f"new_results_{msg_short}"[:80])
        except Exception:
            pass

    # Commit
    if commit and (new_results or True):  # Always save state
        msg = "Auto-sync: " + (", ".join(new_results) if new_results else "update standings & data")
        git_commit_and_push(msg)

    return {
        "total": total,
        "scored": scored + len(updated),
        "new_results": new_results,
        "live_matches": live,
        "updated": len(updated),
    }


# ============================================================
# DATA QUALITY VERIFICATION
# ============================================================
def run_verification() -> dict:
    """Run verify.py checks and save report."""
    try:
        import verify
        report = verify.run_all_checks()
        report['quality_score'] = verify.calculate_quality_score(report)
        # Save report
        verify_path = REPO_DIR / "verify_report.json"
        verify_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        status = report['summary']['status']
        score = report['quality_score']
        print(f"  Data Quality: {score}/100 [{status}] {report['summary']['verdict']}")
        if report['summary']['critical'] > 0:
            for layer_name, layer in report['layers'].items():
                for issue in layer['issues']:
                    if issue['severity'] == 'critical':
                        print(f"    ❌ [{issue['layer']}] {issue.get('issue','?')}")
        return report
    except Exception as e:
        print(f"  [WARN] Verification skipped: {e}")
        return {'summary': {'status': 'UNKNOWN'}, 'quality_score': 0}


# ============================================================
# WATCH MODE - Continuous monitoring
# ============================================================
def watch_mode():
    """Continuously monitor for match results."""
    print("""
╔══════════════════════════════════════════════════════════════╗
║         ⚽ 2026 FIFA WORLD CUP - LIVE MONITOR ⚽            ║
║                                                              ║
║  持续监控中... 每场赛后自动同步                               ║
║  Continuous monitoring... Auto-sync after each match        ║
║                                                              ║
║  Press Ctrl+C to stop                                       ║
╚══════════════════════════════════════════════════════════════╝
""")
    consecutive_no_change = 0
    while True:
        try:
            bump_counter()  # Update counter BEFORE sync (included in commit)
            result = sync_once(commit=True)

            if result["updated"] > 0:
                print(f"\n  🎉 {result['updated']} new results synced!")
                consecutive_no_change = 0
            elif result["live_matches"]:
                print(f"\n  ⏳ {len(result['live_matches'])} match(es) in progress. Waiting...")
                consecutive_no_change = 0
            else:
                consecutive_no_change += 1

            # Dynamic polling: faster during live matches, slower when idle
            if result["live_matches"]:
                wait = POLL_INTERVAL
            else:
                wait = WATCH_INTERVAL

            if consecutive_no_change > 30:  # ~1 hour idle
                wait = 300

            print(f"  Next check in {wait}s...")
            time.sleep(wait)

        except KeyboardInterrupt:
            print("\n\n  Monitor stopped.")
            break
        except Exception as e:
            print(f"  [ERROR] {e}")
            time.sleep(60)


# ============================================================
# WEB DASHBOARD SERVER
# ============================================================
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>⚽ 2026 世界杯实时看板 | FIFA World Cup 2026 Live</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',system-ui,-apple-system,sans-serif;background:#0f1724;color:#e2e8f0;min-height:100vh}
.header{background:linear-gradient(135deg,#1a1a2e,#16213e,#0f3460);padding:20px;text-align:center;border-bottom:3px solid #e2b04a}
.header h1{font-size:2em;color:#e2b04a;margin-bottom:5px}
.header .subtitle{color:#94a3b8;font-size:.9em}
.last-update{text-align:center;color:#64748b;padding:8px;font-size:.8em}
.container{max-width:1400px;margin:0 auto;padding:15px}

/* LIVE banner */
.live-banner{background:#dc2626;color:white;text-align:center;padding:8px;font-weight:bold;display:none;animation:pulse 2s infinite}
.live-banner.active{display:block}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.7}}
.live-dot{display:inline-block;width:10px;height:10px;background:#fff;border-radius:50%;margin-right:6px;animation:blink 1s infinite}
@keyframes blink{0%,100%{opacity:1}50%{opacity:0}}

/* Groups grid */
.groups-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:15px;margin-top:15px}
.group-card{background:#1e293b;border-radius:12px;overflow:hidden;border:1px solid #334155}
.group-header{background:linear-gradient(90deg,#1e40af,#3b82f6);padding:10px 15px;font-weight:bold;font-size:1.1em;color:#fff}
.group-table{width:100%;border-collapse:collapse}
.group-table th{background:#0f1724;padding:6px 8px;font-size:.75em;text-transform:uppercase;color:#94a3b8;text-align:center}
.group-table td{padding:8px;border-bottom:1px solid #1e293b;font-size:.9em}
.group-table tr:hover{background:#334155}
.team-col{text-align:left;font-weight:500}
.num-col{text-align:center;width:28px}
.pts{font-weight:bold;color:#e2b04a}

/* Matches section */
.section-title{font-size:1.3em;color:#e2b04a;margin:25px 0 10px;padding-bottom:8px;border-bottom:2px solid #e2b04a}
.matches-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(350px,1fr));gap:12px}
.match-card{background:#1e293b;border-radius:10px;padding:15px;border:1px solid #334155;transition:all .2s}
.match-card:hover{border-color:#e2b04a}
.match-card.live{border:2px solid #dc2626;animation:glow 2s infinite}
@keyframes glow{0%,100%{box-shadow:0 0 5px #dc2626}50%{box-shadow:0 0 15px #dc2626}}
.match-card.finished{opacity:.85}
.match-date{font-size:.8em;color:#64748b;margin-bottom:8px}
.match-teams{display:flex;align-items:center;justify-content:space-between;gap:12px}
.team{flex:1;text-align:center}
.team-name{font-weight:600;font-size:.95em}
.team-flag{font-size:1.5em}
.score{font-size:1.8em;font-weight:bold;min-width:70px;text-align:center;color:#f8fafc}
.score.pending{color:#64748b;font-size:1em}
.match-info{text-align:center;color:#64748b;font-size:.75em;margin-top:8px}
.match-status{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.7em;font-weight:bold;margin-top:5px}
.status-live{background:#dc2626;color:#fff}
.status-ft{background:#16a34a;color:#fff}
.status-scheduled{background:#334155;color:#94a3b8}

/* Bracket */
.bracket-container{margin-top:20px;overflow-x:auto}
.bracket-svg{min-width:1000px;width:100%;height:auto}

/* Tabs */
.tabs{display:flex;gap:5px;margin:15px 0;flex-wrap:wrap}
.tab{padding:8px 20px;background:#1e293b;border:1px solid #334155;border-radius:8px 8px 0 0;cursor:pointer;color:#94a3b8;font-size:.9em}
.tab.active{background:#3b82f6;color:#fff;border-color:#3b82f6}
.tab-content{display:none}
.tab-content.active{display:block}

/* Responsive */
@media(max-width:768px){
  .groups-grid{grid-template-columns:1fr}
  .matches-grid{grid-template-columns:1fr}
  .header h1{font-size:1.4em}
}
</style>
</head>
<body>

<div class="header">
  <h1>⚽ 2026 FIFA 世界杯实时看板</h1>
  <div class="subtitle">Canada 🇨🇦 · USA 🇺🇸 · Mexico 🇲🇽 | 6月11日 - 7月19日 | 48队 · 104场</div>
</div>

<div class="live-banner" id="liveBanner">
  <span class="live-dot"></span> 比赛进行中 · LIVE MATCHES IN PROGRESS
</div>

<div class="last-update" id="lastUpdate">加载中...</div>

<div class="container">

<div class="tabs">
  <div class="tab active" onclick="switchTab('standings')">📊 积分榜</div>
  <div class="tab" onclick="switchTab('matches')">📅 比赛</div>
  <div class="tab" onclick="switchTab('live')">🔴 直播中</div>
  <div class="tab" onclick="switchTab('bracket')">🏆 淘汰赛对阵</div>
</div>

<!-- Standings Tab -->
<div class="tab-content active" id="tab-standings">
  <div class="groups-grid" id="groupsGrid"></div>
</div>

<!-- Matches Tab -->
<div class="tab-content" id="tab-matches">
  <div id="matchesByDate"></div>
</div>

<!-- Live Tab -->
<div class="tab-content" id="tab-live">
  <div class="matches-grid" id="liveMatches"></div>
</div>

<!-- Bracket Tab -->
<div class="tab-content" id="tab-bracket">
  <div class="bracket-container" id="bracketView"></div>
</div>

</div>

<script>
// ==================== DATA ====================
let matchData = [];
let standings = {};
let currentTab = 'standings';

// ==================== FETCH ====================
async function loadData() {
  try {
    const [mRes, sRes] = await Promise.all([
      fetch('match_data.json'),
      fetch('standings.json')
    ]);
    matchData = await mRes.json();
    standings = await sRes.json();
    renderAll();
    document.getElementById('lastUpdate').textContent =
      '最后更新: ' + new Date().toLocaleString('zh-CN');
  } catch(e) {
    console.error('Failed to load data:', e);
    document.getElementById('lastUpdate').textContent = '加载数据失败，正在重试...';
  }
}

// ==================== RENDER ====================
function renderAll() {
  renderStandings();
  renderMatches();
  renderLive();
  renderBracket();
}

function renderStandings() {
  const grid = document.getElementById('groupsGrid');
  const order = 'ABCDEFGHIJKL'.split('');
  let html = '';
  for (const g of order) {
    const table = standings[g] || [];
    if (!table.length) continue;
    html += `<div class="group-card">
      <div class="group-header">🏆 ${g}组 Group ${g}</div>
      <table class="group-table">
        <tr><th>#</th><th>球队</th><th>赛</th><th>胜</th><th>平</th><th>负</th><th>GF</th><th>GA</th><th>GD</th><th>分</th></tr>`;
    table.forEach((t, i) => {
      html += `<tr>
        <td class="num-col">${i+1}</td>
        <td class="team-col">${t.team}</td>
        <td class="num-col">${t.gp}</td>
        <td class="num-col">${t.w}</td>
        <td class="num-col">${t.d}</td>
        <td class="num-col">${t.l}</td>
        <td class="num-col">${t.gf}</td>
        <td class="num-col">${t.ga}</td>
        <td class="num-col">${t.gd>0?'+':''}${t.gd}</td>
        <td class="num-col pts">${t.pts}</td>
      </tr>`;
    });
    html += '</table></div>';
  }
  grid.innerHTML = html;
}

function renderMatches() {
  const container = document.getElementById('matchesByDate');
  // Group by date
  const byDate = {};
  matchData.forEach(m => {
    const d = m.date || '?';
    if (!byDate[d]) byDate[d] = [];
    byDate[d].push(m);
  });

  const sortedDates = Object.keys(byDate).sort();
  let html = '';
  for (const date of sortedDates) {
    const matches = byDate[date];
    const [y,m,d] = date.split('-');
    html += `<div class="section-title">📅 ${m}月${d}日 (${matches.length}场)</div>`;
    html += '<div class="matches-grid">';
    matches.forEach(m => {
      const isLive = !m.is_result && m.home_score !== null && m.home_score !== undefined;
      const isDone = m.is_result;
      const cardClass = isLive ? 'live' : (isDone ? 'finished' : '');
      const statusHtml = isLive ? '<span class="match-status status-live">🔴 LIVE</span>' :
                         isDone ? '<span class="match-status status-ft">✅ FT</span>' :
                         '<span class="match-status status-scheduled">⏰ 未开始</span>';
      html += `<div class="match-card ${cardClass}">
        <div class="match-date">${m.date} ${m.time||''} ${m.tz||''} · ${m.group||''}组 · ${m.venue||''}</div>
        <div class="match-teams">
          <div class="team"><div class="team-name">${m.home_team}</div></div>
          <div class="score ${!isDone&&!isLive?'pending':''}">${isDone||isLive ? m.home_score+'-'+m.away_score : 'vs'}</div>
          <div class="team"><div class="team-name">${m.away_team}</div></div>
        </div>
        <div class="match-info">${statusHtml}</div>
      </div>`;
    });
    html += '</div>';
  }
  container.innerHTML = html;
}

function renderLive() {
  const container = document.getElementById('liveMatches');
  const liveMatches = matchData.filter(m => !m.is_result && m.home_score !== null && m.home_score !== undefined);
  const banner = document.getElementById('liveBanner');
  if (liveMatches.length > 0) {
    banner.classList.add('active');
    let html = '';
    liveMatches.forEach(m => {
      html += `<div class="match-card live">
        <div class="match-date">${m.date} ${m.time} ${m.tz} · ${m.group}组 · ${m.venue}</div>
        <div class="match-teams">
          <div class="team"><div class="team-name">${m.home_team}</div></div>
          <div class="score">${m.home_score}-${m.away_score}</div>
          <div class="team"><div class="team-name">${m.away_team}</div></div>
        </div>
        <div class="match-info"><span class="match-status status-live">🔴 LIVE</span></div>
      </div>`;
    });
    container.innerHTML = html;
  } else {
    banner.classList.remove('active');
    container.innerHTML = '<div style="text-align:center;color:#64748b;padding:40px;">暂无进行中的比赛<br>No live matches at the moment</div>';
  }
}

function renderBracket() {
  const container = document.getElementById('bracketView');
  // Build knockout bracket from cup_finals pattern
  container.innerHTML = `
    <div style="text-align:center;color:#94a3b8;padding:30px;">
      <h3>🏆 淘汰赛阶段</h3>
      <p style="margin-top:10px;">小组赛结束后揭晓对阵</p>
      <table style="margin:20px auto;text-align:left;border-collapse:collapse;">
        <tr><td style="padding:8px;color:#e2b04a;">Round of 32</td><td style="padding:8px;">6月28日 - 7月3日</td><td style="padding:8px;color:#64748b;">16场</td></tr>
        <tr><td style="padding:8px;color:#e2b04a;">Round of 16</td><td style="padding:8px;">7月4日 - 7月7日</td><td style="padding:8px;color:#64748b;">8场</td></tr>
        <tr><td style="padding:8px;color:#e2b04a;">Quarter-finals</td><td style="padding:8px;">7月9日 - 7月11日</td><td style="padding:8px;color:#64748b;">4场</td></tr>
        <tr><td style="padding:8px;color:#e2b04a;">Semi-finals</td><td style="padding:8px;">7月14日 - 7月15日</td><td style="padding:8px;color:#64748b;">2场</td></tr>
        <tr><td style="padding:8px;color:#e2b04a;">3rd Place</td><td style="padding:8px;">7月18日 @ 迈阿密</td><td style="padding:8px;color:#64748b;">1场</td></tr>
        <tr><td style="padding:8px;color:#e2b04a;font-size:1.2em;">🏆 FINAL</td><td style="padding:8px;font-size:1.1em;">7月19日 @ 纽约/新泽西</td><td style="padding:8px;color:#e2b04a;font-weight:bold;">1场</td></tr>
      </table>
    </div>`;
}

function switchTab(name) {
  currentTab = name;
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  document.querySelector(`.tab:nth-child(${{standings:1,matches:2,live:3,bracket:4}[name]})`).classList.add('active');
  document.getElementById(`tab-${name}`).classList.add('active');
}

// ==================== INIT ====================
loadData();
setInterval(loadData, 60000); // Auto-refresh every 60s
</script>
</body>
</html>"""


def ensure_dashboard():
    """Ensure dashboard.html exists (no longer embedded - read from file)."""
    dash_path = REPO_DIR / "dashboard.html"
    if not dash_path.exists():
        print(f"  [WARN] dashboard.html not found at {dash_path}")
        print(f"  Run with --serve once to verify, or create dashboard.html manually.")
    return dash_path.exists()


def serve_dashboard(port: int = 8888):
    """Start a simple HTTP server for the dashboard."""
    import http.server
    import socketserver

    # Ensure data files exist
    if not DATA_JSON.exists():
        sync_once(commit=False)

    ensure_dashboard()

    os.chdir(str(REPO_DIR))

    class Handler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, format, *args):
            print(f"  [WEB] {args[0]}")

        def do_GET(self):
            if self.path.startswith('/counter/'):
                self._handle_counter(self.path)
            elif self.path.startswith('/visitor'):
                self._handle_visitor(self.path)
            elif self.path == '/live-scores':
                self._handle_live_scores()
            elif self.path == '/health':
                self._handle_health()
            else:
                super().do_GET()

        def _handle_visitor(self, path):
            import datetime, urllib.parse
            visitor_path = REPO_DIR / 'visitors.json'
            try:
                if visitor_path.exists():
                    v = json.loads(visitor_path.read_text(encoding='utf-8'))
                else:
                    v = {'count': 0, 'visits': [], 'tz_dist': {}, 'lang_dist': {}}

                # Parse query params: /visitor?tz=Asia/Shanghai&lang=zh-CN
                parsed = urllib.parse.urlparse(path)
                params = urllib.parse.parse_qs(parsed.query)
                tz = params.get('tz', ['Unknown'])[0][:80]
                lang = params.get('lang', ['Unknown'])[0][:20]

                v['count'] += 1
                v['visits'].append({
                    'time': datetime.datetime.now().isoformat()[:19],
                    'tz': tz,
                    'lang': lang,
                })
                # Keep last 200 entries
                if len(v['visits']) > 200:
                    v['visits'] = v['visits'][-200:]

                # Update distributions
                v['tz_dist'][tz] = v['tz_dist'].get(tz, 0) + 1
                v['lang_dist'][lang] = v['lang_dist'].get(lang, 0) + 1

                visitor_path.write_text(json.dumps(v, ensure_ascii=False, indent=2), encoding='utf-8')
                cnt = v.get('count', 0)
                print(f'  [VIS] #{cnt} tz={tz} lang={lang}')

                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'ok': True, 'count': v['count']}).encode())
            except Exception as e:
                print(f'  [VIS] Error: {e}')
                self.send_response(500)
                self.end_headers()

        def _handle_live_scores(self):
            """Return live scores: ESPN API direct + match_data.json merge (10s cache)."""
            import datetime, time as _time
            try:
                # Cache: re-fetch ESPN max every 10 seconds
                cache_key = '_live_cache'
                now_ts = _time.time()
                if not hasattr(self, cache_key) or now_ts - getattr(self, cache_key + '_ts', 0) > 10:
                    from sync_worldcup import fetch_espn_matches
                    espn = fetch_espn_matches()
                    setattr(self, cache_key, espn)
                    setattr(self, cache_key + '_ts', now_ts)
                else:
                    espn = getattr(self, cache_key, [])

                # Merge with match_data.json
                mpath = REPO_DIR / 'match_data.json'
                matches = json.loads(mpath.read_text(encoding='utf-8')) if mpath.exists() else []
                live = []
                now = datetime.datetime.now(datetime.timezone.utc)
                from datetime import timedelta

                for m in matches:
                    if not m.get('utc_ts'): continue
                    ko = datetime.datetime.strptime(m['utc_ts'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc)
                    if not (ko <= now <= ko + timedelta(hours=2.5)): continue

                    # Try to get live score from ESPN cache
                    hs, aws, status = m.get('home_score'), m.get('away_score'), ''
                    for e in espn:
                        comps = e.get('competitions', [{}])[0]
                        teams = comps.get('competitors', [])
                        if len(teams) < 2: continue
                        enames = [t.get('team', {}).get('displayName', '') for t in teams]
                        if m['home_team'] in enames[0] and m['away_team'] in enames[1]:
                            hs = teams[0].get('score'); aws = teams[1].get('score')
                            status = comps.get('status', {}).get('type', {}).get('name', '')
                            break

                    if hs is not None: hs = int(hs)
                    if aws is not None: aws = int(aws)
                    live.append({
                        'home_team': m['home_team'], 'away_team': m['away_team'],
                        'home_score': hs, 'away_score': aws,
                        'is_result': m.get('is_result', False),
                        'status': status,
                    })

                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'updated': datetime.datetime.now().isoformat(), 'live': live}, ensure_ascii=False).encode())
            except Exception as e:
                err_msg = str(e)
                # Client disconnect (10053/10054) is expected — don't spam logs
                if '10053' not in err_msg and '10054' not in err_msg and 'aborted' not in err_msg.lower():
                    print(f'  [LIVE] Error: {err_msg[:100]}')
                try: self.send_response(500)
                except: pass
                try: self.end_headers()
                except: pass
                self.end_headers()

        def _handle_health(self):
            """Health check — data freshness + push status."""
            import datetime
            try:
                mpath = REPO_DIR / 'match_data.json'
                mtime = datetime.datetime.fromtimestamp(mpath.stat().st_mtime)
                age_min = (datetime.datetime.now() - mtime).total_seconds() / 60
                matches = json.loads(mpath.read_text(encoding='utf-8'))
                scored = sum(1 for m in matches if m.get('is_result'))
                status = 'ok' if age_min < 15 else 'stale'

                # Push failure log
                push_log = REPO_DIR / 'push_failures.log'
                push_fails = 0
                last_fail = ''
                if push_log.exists():
                    lines = push_log.read_text(encoding='utf-8').strip().split('\n')
                    push_fails = len(lines)
                    last_fail = lines[-1] if lines else ''

                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'status': status, 'scored': scored, 'total': len(matches),
                    'data_age_min': round(age_min, 1), 'updated': mtime.isoformat(),
                    'push_failures': push_fails, 'last_push_fail': last_fail,
                }, ensure_ascii=False).encode())
            except Exception as e:
                self.send_response(500)
                self.end_headers()

        def _handle_counter(self, path):
            import datetime
            counter_path = REPO_DIR / 'counter.json'
            try:
                if counter_path.exists():
                    c = json.loads(counter_path.read_text(encoding='utf-8'))
                else:
                    c = {'uv': 0, 'pv': 0, 'updated': ''}

                if '/uv' in path:
                    c['uv'] += 1
                if '/pv' in path:
                    c['pv'] += 1

                c['updated'] = datetime.datetime.now().isoformat()
                counter_path.write_text(json.dumps(c, ensure_ascii=False, indent=2), encoding='utf-8')
                print(f'  [CTR] UV={c["uv"]} PV={c["pv"]}')

                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(json.dumps({'ok': True, 'uv': c['uv'], 'pv': c['pv']}).encode())
            except Exception as e:
                print(f'  [CTR] Error: {e}')
                self.send_response(500)
                self.end_headers()

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║     ⚽ 2026 WORLD CUP DASHBOARD ⚽                          ║
║                                                              ║
║     看板地址: http://localhost:{port}                          ║
║     Dashboard: http://localhost:{port}/dashboard.html         ║
║                                                              ║
║     自动刷新: 每60秒                                         ║
║     后台同步: 持续运行                                        ║
║                                                              ║
║     Press Ctrl+C to stop                                     ║
╚══════════════════════════════════════════════════════════════╝
""")

    with socketserver.TCPServer(("", port), Handler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n  Server stopped.")


# ============================================================
# TUI MODE - Terminal Dashboard with Rich
# ============================================================
def tui_mode():
    """Interactive terminal dashboard using rich library."""
    try:
        from rich.console import Console
        from rich.table import Table
        from rich.live import Live
        from rich.layout import Layout
        from rich.panel import Panel
        from rich.text import Text
        from rich.align import Align
        from rich import box
    except ImportError:
        print("The 'rich' library is required for TUI mode.")
        print("Install it with: pip install rich")
        sys.exit(1)

    console = Console()
    tui_data = {"matches": [], "standings": {}, "live": []}

    def refresh_data():
        """Fetch latest data without committing to git."""
        try:
            # Light sync - just fetch and parse, don't commit
            espn = fetch_espn_matches()
            tsdb = fetch_thesportsdb_matches()
            all_api = espn + tsdb
            matches = parse_cup_txt()
            merge_api_results(matches, all_api)  # TUI: display only, discard corrections
            matches = parse_cup_txt()  # Re-parse after merge
            standings = calculate_standings(matches)
            live = [m for m in espn if "FULL_TIME" not in m.get("status", "") and
                    m.get("status") not in ("STATUS_SCHEDULED", "STATUS_FINAL")]
            tui_data["matches"] = matches
            tui_data["standings"] = standings
            tui_data["live"] = live
            tui_data["espn"] = espn
        except Exception as e:
            pass  # Silently handle - keep last known data

    def build_layout() -> Layout:
        """Build the terminal layout."""
        layout = Layout()
        layout.split(
            Layout(name="header", size=3),
            Layout(name="main"),
            Layout(name="footer", size=3),
        )
        layout["main"].split_row(
            Layout(name="left", ratio=2),
            Layout(name="right", ratio=3),
        )
        return layout

    def make_header() -> Panel:
        """Build header panel."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        live_count = len(tui_data.get("live", []))
        matches = tui_data.get("matches", [])
        scored = sum(1 for m in matches if m.get("is_result"))
        total = len(matches)

        text = Text()
        text.append("⚽ 2026 FIFA WORLD CUP · 世界杯实时终端看板", style="bold yellow")
        text.append(f"\n🇨🇦🇺🇸🇲🇽 Canada · USA · Mexico | {now} | ")
        if live_count > 0:
            text.append(f"🔴 {live_count} LIVE", style="bold red")
        else:
            text.append("No live matches", style="dim")
        text.append(f" | Results: {scored}/{total}")
        return Panel(text, box=box.HEAVY, border_style="yellow")

    def make_standings_panel() -> Panel:
        """Build compact standings table."""
        standings = tui_data.get("standings", {})
        table = Table(box=box.SIMPLE, padding=(0, 1), collapse_padding=True)
        table.add_column("G", style="bold cyan", width=2)
        table.add_column("Team / 球队", style="white", width=20)
        table.add_column("P", style="dim", width=2, justify="right")
        table.add_column("W", style="dim", width=2, justify="right")
        table.add_column("D", style="dim", width=2, justify="right")
        table.add_column("L", style="dim", width=2, justify="right")
        table.add_column("GF", style="dim", width=2, justify="right")
        table.add_column("GA", style="dim", width=2, justify="right")
        table.add_column("GD", style="dim", width=3, justify="right")
        table.add_column("Pts", style="bold yellow", width=3, justify="right")

        for g in "ABCDEFGHIJKL":
            teams = standings.get(g, [])
            if not teams:
                continue
            for i, t in enumerate(teams):
                gd_str = f"+{t['gd']}" if t['gd'] > 0 else str(t['gd'])
                style = "bold" if i < 2 else ""
                cn = team_cn(t['team'])
                name = f"{cn} {t['team']}" if cn != t['team'] else t['team']
                table.add_row(
                    g if i == 0 else "",
                    name,
                    str(t["gp"]), str(t["w"]), str(t["d"]), str(t["l"]),
                    str(t["gf"]), str(t["ga"]), gd_str,
                    str(t["pts"]),
                    style=style,
                )

        return Panel(table, title="📊 Standings · 积分榜", border_style="blue")

    def make_matches_panel() -> Panel:
        """Build recent & upcoming matches."""
        matches = tui_data.get("matches", [])
        table = Table(box=box.SIMPLE, padding=(0, 1), collapse_padding=True)
        table.add_column("Date", style="dim", width=10)
        table.add_column("Group", style="cyan", width=3)
        table.add_column("Home / 主", style="white", width=16)
        table.add_column("Score", style="bold", width=7, justify="center")
        table.add_column("Away / 客", style="white", width=16)
        table.add_column("Status", width=8)

        # Show: today + next 2 days of matches, plus recent results
        shown = set()
        today = datetime.now().strftime("%Y-%m-%d")

        # Sort: today's matches first, then upcoming, then past with results
        def sort_key(m):
            d = m.get("date", "9999")
            is_result = m.get("is_result")
            if d < today and is_result:
                return (0, d)  # Past results
            elif d == today:
                return (1, m.get("time", ""))  # Today
            else:
                return (2, d, m.get("time", ""))  # Future

        sorted_matches = sorted(matches, key=sort_key)

        # Show last 5 results + today + next 5
        display = []
        past_results = [m for m in sorted_matches if m.get("is_result") and m["date"] < today]
        today_matches = [m for m in sorted_matches if m["date"] == today]
        upcoming = [m for m in sorted_matches if m["date"] > today and not m.get("is_result")]

        display = past_results[-6:] + today_matches + upcoming[:8]

        for m in display:
            d = m["date"]
            hs = m.get("home_score")
            aws = m.get("away_score")
            is_r = m.get("is_result")

            if is_r:
                score = f"{hs}-{aws}"
                status = "[green]FT[/green]"
            elif hs is not None and not is_r:
                score = f"[red]{hs}-{aws}[/red]"
                status = "[red]LIVE[/red]"
            else:
                score = "vs"
                status = "[dim]·[/dim]"

            key = f"{d}{m['home_team']}{m['away_team']}"
            if key in shown:
                continue
            shown.add(key)

            # Shorten date & use Chinese names
            date_short = d[5:]  # MM-DD
            h_name = team_cn(m["home_team"])
            a_name = team_cn(m["away_team"])
            table.add_row(date_short, m.get("group", ""),
                         h_name, score, a_name, status)

        return Panel(table, title="📅 Matches · 比赛", border_style="green")

    def make_live_panel() -> Panel:
        """Build live match panel."""
        live = tui_data.get("live", [])
        if not live:
            return Panel(
                Align.center(Text("No live matches · 暂无直播比赛", style="dim")),
                title="🔴 Live · 直播", border_style="red"
            )

        table = Table(box=box.SIMPLE, padding=(0, 2))
        table.add_column("Match", style="bold white")
        table.add_column("Score", style="bold red", justify="center")
        table.add_column("Time", style="yellow")
        table.add_column("Venue", style="dim")

        for m in live:
            hs = m.get("home_score", "?")
            aws = m.get("away_score", "?")
            clock = m.get("clock", "")
            h_cn = team_cn(m['home_team'])
            a_cn = team_cn(m['away_team'])
            table.add_row(
                f"{h_cn} {m['home_team']}  vs  {a_cn} {m['away_team']}",
                f"{hs} - {aws}",
                clock,
                m.get("venue", "")[:30],
            )

        return Panel(table, title="🔴 Live Now · 正在直播", border_style="red")

    def make_footer() -> Panel:
        """Build footer with keybindings."""
        text = Text()
        text.append("q", style="bold yellow")
        text.append(" quit/退出  ", style="dim")
        text.append("r", style="bold yellow")
        text.append(" refresh/刷新  ", style="dim")
        text.append("1", style="bold yellow")
        text.append(" standings  ", style="dim")
        text.append("2", style="bold yellow")
        text.append(" matches  ", style="dim")
        text.append("3", style="bold yellow")
        text.append(" live  ", style="dim")
        text.append("Ctrl+C", style="bold yellow")
        text.append(" exit", style="dim")
        return Panel(text, box=box.SIMPLE, border_style="dim")

    # Initial data load
    refresh_data()

    # For keyboard input without blocking
    import threading
    import select

    running = True
    current_view = "all"  # all, standings, matches, live

    def keyboard_listener():
        nonlocal running, current_view
        # Windows compatible input
        import msvcrt
        while running:
            try:
                if msvcrt.kbhit():
                    key = msvcrt.getch().decode("utf-8", errors="ignore").lower()
                    if key == "q":
                        running = False
                    elif key == "r":
                        refresh_data()
                    elif key == "1":
                        current_view = "standings"
                    elif key == "2":
                        current_view = "matches"
                    elif key == "3":
                        current_view = "all"
            except Exception:
                pass
            time.sleep(0.1)

    kbd_thread = threading.Thread(target=keyboard_listener, daemon=True)
    kbd_thread.start()

    # Main render loop
    with Live(console=console, screen=True, refresh_per_second=2) as live:
        last_refresh = time.time()
        while running:
            # Auto-refresh data every 60 seconds
            if time.time() - last_refresh > 60:
                refresh_data()
                last_refresh = time.time()

            layout = build_layout()
            layout["header"].update(make_header())

            if current_view == "standings":
                layout["main"].update(make_standings_panel())
            elif current_view == "matches":
                layout["main"].update(make_matches_panel())
            else:
                layout["left"].update(make_standings_panel())
                right_layout = Layout()
                right_layout.split(
                    Layout(make_live_panel(), size=6),
                    Layout(make_matches_panel()),
                )
                layout["right"].update(right_layout)

            layout["footer"].update(make_footer())
            live.update(layout)

            time.sleep(0.5)

    console.print("\n[bold]Goodbye! 👋[/bold]")


# ============================================================
# TEXT MODE - One-shot terminal output
# ============================================================
def text_mode():
    """Print a one-shot text dashboard to terminal (no screen takeover)."""
    try:
        from rich.console import Console
        from rich.table import Table
        from rich.panel import Panel
        from rich.text import Text
        from rich import box
    except ImportError:
        print("The 'rich' library is required. Install: pip install rich")
        sys.exit(1)

    console = Console()
    matches = parse_cup_txt()
    standings = calculate_standings(matches)
    espn = fetch_espn_matches()
    live = [m for m in espn if "FULL_TIME" not in m.get("status", "") and
            m.get("status") not in ("STATUS_SCHEDULED", "STATUS_FINAL")]

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    scored = sum(1 for m in matches if m.get("is_result"))

    # Header
    console.print()
    console.rule(f"⚽ 2026 FIFA WORLD CUP · 世界杯 | {now} | {scored}/104 results")
    if live:
        console.print(f"[bold red]🔴 LIVE: {len(live)} match(es) in progress[/bold red]")
    console.print()

    # Standings
    table = Table(title="📊 Group Standings · 积分榜", box=box.SIMPLE, padding=(0, 1))
    table.add_column("G", style="cyan", width=2)
    table.add_column("Team / 球队", width=14)
    table.add_column("P", width=2, justify="right")
    table.add_column("W", width=2, justify="right")
    table.add_column("D", width=2, justify="right")
    table.add_column("L", width=2, justify="right")
    table.add_column("GF", width=2, justify="right")
    table.add_column("GA", width=2, justify="right")
    table.add_column("GD", width=3, justify="right")
    table.add_column("Pts", style="bold yellow", width=3, justify="right")

    for g in "ABCDEFGHIJKL":
        teams = standings.get(g, [])
        if not teams:
            continue
        for i, t in enumerate(teams):
            gd_str = f"+{t['gd']}" if t['gd'] > 0 else str(t['gd'])
            name = team_cn(t['team'])  # Chinese name in text mode
            table.add_row(
                g if i == 0 else "", name,
                str(t["gp"]), str(t["w"]), str(t["d"]), str(t["l"]),
                str(t["gf"]), str(t["ga"]), gd_str, str(t["pts"]),
                style="bold" if i < 2 else "",
            )
    console.print(table)

    # Live matches
    if live:
        console.print()
        for m in live:
            h_cn = team_cn(m['home_team'])
            a_cn = team_cn(m['away_team'])
            console.print(Panel(
                f"[bold white]{h_cn}[/bold white]  [bold red]{m.get('home_score','?')} - {m.get('away_score','?')}[/bold red]  [bold white]{a_cn}[/bold white]\n"
                f"[dim]⏱ {m.get('clock','')}  |  {m.get('venue','')}[/dim]",
                title="🔴 LIVE", border_style="red"
            ))

    # Today's & recent matches
    console.print()
    today = datetime.now().strftime("%Y-%m-%d")
    recent = [m for m in matches if m.get("is_result")]
    upcoming_today = [m for m in matches if m["date"] == today and not m.get("is_result")]

    if recent:
        console.print(f"[bold]Recent Results / 最近赛果:[/bold]")
        for m in recent[-8:]:
            h_cn = team_cn(m['home_team'])
            a_cn = team_cn(m['away_team'])
            console.print(
                f"  {m['date']}  {h_cn} [bold]{m['home_score']}-{m['away_score']}[/bold] {a_cn}  "
                f"[dim]({m['group']}组)[/dim]"
            )

    if upcoming_today:
        console.print(f"\n[bold]Today's Matches / 今日比赛:[/bold]")
        for m in upcoming_today:
            h_cn = team_cn(m['home_team'])
            a_cn = team_cn(m['away_team'])
            console.print(f"  {m['time']} {m['tz']}  {h_cn} vs {a_cn}  @ {m['venue']}")

    console.print()
    console.print("[dim]Use --tui for interactive mode / 使用 --tui 进入交互模式[/dim]")
    console.print()


# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    ensure_dashboard()

    if "--teams" in sys.argv:
        build_teams_data(force_refresh=True)
        print("\n✅ Teams data built. Run --text or --serve to view.")
    elif "--text" in sys.argv:
        text_mode()
    elif "--tui" in sys.argv or "-t" in sys.argv:
        tui_mode()
    elif "--watch" in sys.argv:
        # Background sync thread + foreground web server
        import threading
        sync_thread = threading.Thread(target=watch_mode, daemon=True)
        sync_thread.start()
        serve_dashboard()
    elif "--serve" in sys.argv:
        # Single sync loop + web server (no decoupled complexity)
        import threading
        def bg_sync():
            pm_last_check = ''
            while True:
                try:
                    bump_counter()
                    sync_once(commit=True)
                    today = __import__('datetime').date.today().isoformat()
                    if today != pm_last_check:
                        fetch_pm_odds()
                        pm_last_check = today
                except Exception as e:
                    print(f"  [SYNC ERROR] {e}")
                time.sleep(WATCH_INTERVAL)
        threading.Thread(target=bg_sync, daemon=True).start()
        serve_dashboard()
    elif "--once" in sys.argv:
        sync_once(commit=False)
    else:
        # Default: sync once + commit
        sync_once(commit=True)
        print("\n✅ Sync complete. Use --watch for continuous mode, --serve for web dashboard.")
