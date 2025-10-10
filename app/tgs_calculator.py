# app/tgs_calculator.py

import asyncio
import json
from typing import Any, Dict, Optional, List, Tuple
from collections import defaultdict
from datetime import datetime, timedelta

# Gerekli collector fonksiyonlarını import et
try:
    from app.collector import (
        fetch_all_event_details,
        fetch_player_matches,
        fetch_rankings_via_page,
        fetch_year_statistics,
        fetch_scheduled_events_for_dates
    )
except (ImportError, ModuleNotFoundError):
    # Bu blok, script'i tek başına çalıştırırken veya collector bulunamadığında hata vermesini önler
    print("UYARI: 'app.collector' bulunamadı. Sahte (mock) fonksiyonlar kullanılıyor.")
    async def fetch_all_event_details(*args, **kwargs): return [{"error": "mock"}]*2
    async def fetch_player_matches(team_id, page=0): return {"events": [], "hasNextPage": False}
    async def fetch_rankings_via_page(*args, **kwargs): return {"rankings": []}
    async def fetch_year_statistics(*args, **kwargs): return {"statistics": []}
    async def fetch_scheduled_events_for_dates(*args, **kwargs): return {"events": []}

# --- Model Ağırlıkları ---
WEIGHTS = {
    "oran": 0.25,
    "sıralama": 0.10,
    "genel_form": 0.05,
    "son_10_mac_formu": 0.05,
    "h2h": 0.075,
    "sentiment": 0.05,
    "yuzey_formu": 0.075,
    "rakip_kalitesi": 0.10,
    "tiebreak_psikolojisi": 0.05,
    "servis_hakimiyeti": 0.10,
    "kritik_anlar_puani": 0.075,
    "hucum_puani": 0.075,
}
# Ağırlıkları normalize et
total_weight = sum(WEIGHTS.values())
if total_weight > 0:
    for k in WEIGHTS:
        WEIGHTS[k] /= total_weight

# --- Yardımcı Fonksiyonlar ---
def fractional_to_decimal(fractional: str) -> float:
    if not fractional or "/" not in fractional:
        return float(fractional or "2.0")
    try:
        num, den = map(int, fractional.split('/'))
        if den == 0: return 2.0
        return 1.0 + (num / den)
    except (ValueError, TypeError):
        return 2.0

# --- Basit TTL Cache (in-memory) ---
# Not: Uygulama yeniden başlatılınca temizlenir. Süreyi kısa tutuyoruz ki bayat veri riski olmasın.
_CACHE_RANKINGS: Dict[int, Tuple[float, Dict[str, Any]]] = {}
_CACHE_MATCHES: Dict[int, Tuple[float, Dict[str, Any]]] = {}
_CACHE_YEAR_STATS: Dict[Tuple[int, int], Tuple[float, Dict[str, Any]]] = {}
_CACHE_EVENT_DETAILS: Dict[int, Tuple[float, Optional[Dict[str, Any]]]] = {}
_CACHE_PRE_MATCH: Dict[Tuple[int, int, int], Tuple[float, Dict[str, Any]]] = {}

def _cache_get(cache: Dict, key, ttl_seconds: int):
    now = asyncio.get_event_loop().time()
    item = cache.get(key)
    if not item:
        return None
    ts, value = item
    if (now - ts) < ttl_seconds:
        return value
    return None

def _cache_put(cache: Dict, key, value):
    now = asyncio.get_event_loop().time()
    cache[key] = (now, value)

# --- Veri Toplama Fonksiyonları ---
async def get_player_stats_for_years(team_id: int, years: List[int]) -> Dict[str, List]:
    # Yıllık istatistikleri kısa süre cache'leyelim (TTL ~ 15 dakika)
    async def get_year(year: int) -> Dict[str, Any]:
        cached = _cache_get(_CACHE_YEAR_STATS, (team_id, year), ttl_seconds=900)
        if cached is not None:
            return cached
        data = await fetch_year_statistics(team_id, year)
        _cache_put(_CACHE_YEAR_STATS, (team_id, year), data)
        return data

    # Tüm istenen yılları koru (doğruluk için)
    results = await asyncio.gather(*[get_year(y) for y in years])
    all_yearly_stats = [stat for year_data in results for stat in year_data.get("statistics", [])]
    return {"all_stats": all_yearly_stats}

async def get_event_details(event_id: int) -> Optional[Dict[str, Any]]:
    cached = _cache_get(_CACHE_EVENT_DETAILS, event_id, ttl_seconds=30)
    if cached is not None:
        return cached
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    dates_to_check = [today.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")]
    
    scheduled_events_data = await fetch_scheduled_events_for_dates(dates_to_check, headless=True)
    
    for event in scheduled_events_data.get("events", []):
        if event.get("id") == event_id:
            result = {
                "home_team_id": event.get("homeTeam", {}).get("id"),
                "away_team_id": event.get("awayTeam", {}).get("id"),
                "home_team_name": event.get("homeTeam", {}).get("name"),
                "away_team_name": event.get("awayTeam", {}).get("name"),
                "ground_type": event.get("groundType"),
            }
            _cache_put(_CACHE_EVENT_DETAILS, event_id, result)
            return result
    return None

async def fetch_all_player_matches(team_id: int, max_pages: int = 0) -> Dict[str, Any]:
    # Oyuncu maçlarını kısa süre cache'le (TTL ~ 5 dakika)
    cached = _cache_get(_CACHE_MATCHES, team_id, ttl_seconds=300)
    if cached is not None:
        return cached

    all_events, page = [], 0
    while True:
        data = await fetch_player_matches(team_id, page=page)
        page_events = data.get("events", [])
        if not page_events: break
        all_events.extend(page_events)
        if not data.get("hasNextPage", False): break
        page += 1
        if max_pages and page >= max_pages:
            break
    unique_events = {event['id']: event for event in all_events}.values()
    result = {"events": sorted(list(unique_events), key=lambda x: x.get('startTimestamp', 0), reverse=True)}
    _cache_put(_CACHE_MATCHES, team_id, result)
    return result

async def get_pre_match_data(event_id: int, home_team_id: int, away_team_id: int) -> Dict[str, Any]:
    cached = _cache_get(_CACHE_PRE_MATCH, (event_id, home_team_id, away_team_id), ttl_seconds=60)
    if cached is not None:
        return cached
    current_year = datetime.now().year
    years_to_fetch = [current_year, current_year - 1, current_year - 2]
    
    async def get_rankings_cached(tid: int):
        cached_r = _cache_get(_CACHE_RANKINGS, tid, ttl_seconds=300)
        if cached_r is not None:
            return cached_r
        data = await fetch_rankings_via_page(tid)
        _cache_put(_CACHE_RANKINGS, tid, data)
        return data

    tasks = {
        "home_rankings": get_rankings_cached(home_team_id),
        "home_matches": fetch_all_player_matches(home_team_id),
        "home_yearly_stats": get_player_stats_for_years(home_team_id, years_to_fetch),
        "away_rankings": get_rankings_cached(away_team_id),
        "away_matches": fetch_all_player_matches(away_team_id),
        "away_yearly_stats": get_player_stats_for_years(away_team_id, years_to_fetch),
    }
    results = await asyncio.gather(*tasks.values())
    data_map = dict(zip(tasks.keys(), results))

    home_data = {"rankings": data_map["home_rankings"], "matches": data_map["home_matches"], "yearly_stats": data_map["home_yearly_stats"]}
    away_data = {"rankings": data_map["away_rankings"], "matches": data_map["away_matches"], "yearly_stats": data_map["away_yearly_stats"]}

    vote_details = await fetch_all_event_details(event_id, ["votes", "odds/1/all"])
    match_details = dict(zip(["votes", "oddsAll"], vote_details))

    result = {"match_details": match_details, "home_player": home_data, "away_player": away_data}
    _cache_put(_CACHE_PRE_MATCH, (event_id, home_team_id, away_team_id), result)
    return result

# --- Skor Hesaplama Fonksiyonu ---
def calculate_metric_scores(data: Dict[str, Any], home_team_id: int, away_team_id: int, ground_type: str) -> tuple[Dict[str, float], Dict[str, float]]:
    home_scores, away_scores = {}, {}

    def aggregate_stats_for_surface(all_stats: List[Dict], surface: str) -> Dict:
        surface_stats = [s for s in all_stats if s.get("groundType") == surface]
        stats_to_aggregate = surface_stats if surface_stats else all_stats
        if not stats_to_aggregate: return defaultdict(float)
        
        aggregated = defaultdict(float)
        for stat_group in stats_to_aggregate:
            for key, value in stat_group.items():
                if isinstance(value, (int, float)): aggregated[key] += value
        return aggregated

    home_yearly = aggregate_stats_for_surface(data['home_player']['yearly_stats']['all_stats'], ground_type)
    away_yearly = aggregate_stats_for_surface(data['away_player']['yearly_stats']['all_stats'], ground_type)

    def calculate_score(home_val, away_val):
        total = home_val + away_val
        return (0.5, 0.5) if total <= 0 else (home_val / total, away_val / total)

    # Metrik hesaplamaları (servis, kritik anlar, hücum vb.)
    home_serve_power = (home_yearly.get('aces', 0) * 1.5 + home_yearly.get('firstServePointsScored', 0) - home_yearly.get('doubleFaults', 0) * 2)
    away_serve_power = (away_yearly.get('aces', 0) * 1.5 + away_yearly.get('firstServePointsScored', 0) - away_yearly.get('doubleFaults', 0) * 2)
    home_scores['servis_hakimiyeti'], away_scores['servis_hakimiyeti'] = calculate_score(home_serve_power, away_serve_power)

    h_tiebreak_total = home_yearly.get('tiebreaksWon', 0) + home_yearly.get('tiebreakLosses', 0)
    a_tiebreak_total = away_yearly.get('tiebreaksWon', 0) + away_yearly.get('tiebreakLosses', 0)
    h_tiebreak_ratio = home_yearly.get('tiebreaksWon', 0) / h_tiebreak_total if h_tiebreak_total > 0 else 0.5
    a_tiebreak_ratio = away_yearly.get('tiebreaksWon', 0) / a_tiebreak_total if a_tiebreak_total > 0 else 0.5
    h_bp_ratio = home_yearly.get('breakPointsScored', 0) / (home_yearly.get('breakPointsTotal') or 1)
    a_bp_ratio = away_yearly.get('breakPointsScored', 0) / (away_yearly.get('breakPointsTotal') or 1)
    home_clutch = (h_tiebreak_ratio + h_bp_ratio) / 2
    away_clutch = (a_tiebreak_ratio + a_bp_ratio) / 2
    home_scores['kritik_anlar_puani'], away_scores['kritik_anlar_puani'] = calculate_score(home_clutch, away_clutch)

    home_attack_ratio = home_yearly.get('winnersTotal', 0) / (home_yearly.get('unforcedErrorsTotal') or 1)
    away_attack_ratio = away_yearly.get('winnersTotal', 0) / (away_yearly.get('unforcedErrorsTotal') or 1)
    home_scores['hucum_puani'], away_scores['hucum_puani'] = calculate_score(home_attack_ratio, away_attack_ratio)

    try:
        ranks = {}
        for player_key in ['home', 'away']:
            player_ranks = data[f'{player_key}_player'].get('rankings', {}).get('rankings', [])
            official = next((r['ranking'] for r in player_ranks if r.get('rankingClass') == 'team'), None)
            utr = next((r['ranking'] for r in player_ranks if r.get('rankingClass') == 'utr'), None)
            valid_ranks = [1/r for r in [official, utr] if r is not None and r > 0]
            ranks[player_key] = (sum(valid_ranks) / len(valid_ranks)) if valid_ranks else 0
        home_scores['sıralama'], away_scores['sıralama'] = calculate_score(ranks.get('home', 0), ranks.get('away', 0))
    except Exception: home_scores['sıralama'], away_scores['sıralama'] = 0.5, 0.5

    try:
        pre_market = next(m for m in data['match_details']['oddsAll'].get('markets', []) if not m.get('isLive') and m.get('marketName') == 'Full time')
        home_odds = fractional_to_decimal(next(c for c in pre_market['choices'] if c['name'] == '1').get('fractionalValue', "2.0"))
        away_odds = fractional_to_decimal(next(c for c in pre_market['choices'] if c['name'] == '2').get('fractionalValue', "2.0"))
        home_prob, away_prob = 1 / home_odds, 1 / away_odds
        home_scores['oran'], away_scores['oran'] = calculate_score(home_prob, away_prob)
    except Exception: home_scores['oran'], away_scores['oran'] = 0.5, 0.5

    try:
        votes = data['match_details']['votes'].get('vote', {})
        home_scores['sentiment'], away_scores['sentiment'] = calculate_score(votes.get('vote1', 0), votes.get('vote2', 0))
    except Exception: home_scores['sentiment'], away_scores['sentiment'] = 0.5, 0.5
    
    all_matches_home = data['home_player'].get('matches', {}).get('events', [])
    all_matches_away = data['away_player'].get('matches', {}).get('events', [])
    home_player_name = data.get('home_team_name', '')
    away_player_name = data.get('away_team_name', '')

    try:
        surface_h2h_home_wins, surface_h2h_away_wins = 0, 0
        for match in all_matches_home:
            opponent_id, is_home_in_past_match = (None, None)
            if str(match.get('homeTeam', {}).get('id')) == str(home_team_id):
                opponent_id, is_home_in_past_match = str(match.get('awayTeam', {}).get('id')), True
            elif str(match.get('awayTeam', {}).get('id')) == str(home_team_id):
                opponent_id, is_home_in_past_match = str(match.get('homeTeam', {}).get('id')), False
            
            if opponent_id == str(away_team_id) and match.get('groundType') == ground_type:
                winner_code = match.get('winnerCode')
                if (is_home_in_past_match and winner_code == 1) or (not is_home_in_past_match and winner_code == 2):
                    surface_h2h_home_wins += 1
                else:
                    surface_h2h_away_wins += 1
        home_scores['h2h'], away_scores['h2h'] = calculate_score(surface_h2h_home_wins, surface_h2h_away_wins)
    except Exception: home_scores['h2h'], away_scores['h2h'] = 0.5, 0.5

    def get_stats_from_matches(matches: List[Dict], player_id: int, player_name: str, limit: Optional[int] = None):
        if limit and len(matches) > limit: matches = matches[:limit]
        stats = defaultdict(float)
        player_last_name = player_name.split(' ')[-1].lower() if player_name else ''
        for event in matches:
            stats['total'] += 1
            winner_code = event.get('winnerCode')
            is_home = (event.get('homeTeam', {}).get('id') == player_id)
            is_away = (event.get('awayTeam', {}).get('id') == player_id)
            is_winner = (is_home and winner_code == 1) or (is_away and winner_code == 2)
            if is_winner:
                stats['wins'] += 1
                opponent = event.get('awayTeam') if is_home else event.get('homeTeam')
                opponent_rank = opponent.get('ranking', 1000)
                if opponent_rank and opponent_rank > 0:
                    stats['quality_score'] += 1000 / opponent_rank
                    stats['quality_wins'] += 1
            if event.get('groundType') == ground_type:
                stats['surface_total'] += 1
                if is_winner: stats['surface_wins'] += 1
            for i in range(1, 6):
                tb_key = f'period{i}TieBreak'
                if tb_key in event.get('homeScore', {}) and tb_key in event.get('awayScore', {}):
                    stats['tb_played'] += 1
                    home_tb, away_tb = event['homeScore'][tb_key], event['awayScore'][tb_key]
                    if (is_home and home_tb > away_tb) or (is_away and away_tb > home_tb):
                        stats['tb_wins'] += 1
        return stats

    home_stats_all = get_stats_from_matches(all_matches_home, home_team_id, home_player_name)
    away_stats_all = get_stats_from_matches(all_matches_away, away_team_id, away_player_name)
    home_stats_last10 = get_stats_from_matches(all_matches_home, home_team_id, home_player_name, limit=10)
    away_stats_last10 = get_stats_from_matches(all_matches_away, away_team_id, away_player_name, limit=10)

    home_scores['genel_form'] = home_stats_all['wins'] / home_stats_all['total'] if home_stats_all['total'] > 0 else 0.5
    away_scores['genel_form'] = away_stats_all['wins'] / away_stats_all['total'] if away_stats_all['total'] > 0 else 0.5
    home_scores['son_10_mac_formu'] = home_stats_last10['wins'] / home_stats_last10['total'] if home_stats_last10['total'] > 0 else 0.5
    away_scores['son_10_mac_formu'] = away_stats_last10['wins'] / away_stats_last10['total'] if away_stats_last10['total'] > 0 else 0.5
    home_scores['yuzey_formu'] = home_stats_all['surface_wins'] / home_stats_all['surface_total'] if home_stats_all['surface_total'] > 0 else 0.5
    away_scores['yuzey_formu'] = away_stats_all['surface_wins'] / away_stats_all['surface_total'] if away_stats_all['surface_total'] > 0 else 0.5
    home_quality_avg = home_stats_all['quality_score'] / home_stats_all['quality_wins'] if home_stats_all['quality_wins'] > 0 else 1
    away_quality_avg = away_stats_all['quality_score'] / away_stats_all['quality_wins'] if away_stats_all['quality_wins'] > 0 else 1
    home_scores['rakip_kalitesi'], away_scores['rakip_kalitesi'] = calculate_score(home_quality_avg, away_quality_avg)
    home_scores['tiebreak_psikolojisi'] = home_stats_all['tb_wins'] / home_stats_all['tb_played'] if home_stats_all['tb_played'] > 0 else 0.5
    away_scores['tiebreak_psikolojisi'] = away_stats_all['tb_wins'] / away_stats_all['tb_played'] if away_stats_all['tb_played'] > 0 else 0.5

    return home_scores, away_scores

# --- Ana Çağrılabilir Fonksiyon ---
async def get_match_prediction(event_id: int) -> Dict[str, Any]:
    """Ana tahmin fonksiyonu: Tüm verileri toplar, hesaplar ve sonucu döndürür."""
    
    # Fonksiyona parametre olarak gelen event_id'nin kullanıldığından emin olun.
    event_info = await get_event_details(event_id)
    if not event_info or not all(key in event_info for key in ["home_team_id", "away_team_id"]):
        return {"error": f"{event_id} ID'li maç detayı bulunamadı."}

    all_data = {**event_info, **await get_pre_match_data(event_id, event_info["home_team_id"], event_info["away_team_id"])}
    
    home_scores, away_scores = calculate_metric_scores(all_data, event_info["home_team_id"], event_info["away_team_id"], event_info["ground_type"])

    home_tgs = sum(WEIGHTS[key] * home_scores.get(key, 0.5) for key in WEIGHTS)
    away_tgs = sum(WEIGHTS[key] * away_scores.get(key, 0.5) for key in WEIGHTS)
    total_tgs = home_tgs + away_tgs
    home_prior = home_tgs / total_tgs if total_tgs > 0 else 0.5
    away_prior = away_tgs / total_tgs if total_tgs > 0 else 0.5

    return {
        "home_player_name": event_info["home_team_name"],
        "away_player_name": event_info["away_team_name"],
        "home_win_prob": home_prior,
        "away_win_prob": away_prior,
        "scores": {"home": home_scores, "away": away_scores},
        "weights": WEIGHTS
    }