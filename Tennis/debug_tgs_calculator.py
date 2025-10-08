import asyncio
import json
from typing import Any, Dict, Optional, List
from collections import defaultdict
from datetime import datetime, timedelta
import time  # Zaman ölçümü için eklendi
from functools import wraps  # Decorator için eklendi

# --- Zaman Ölçüm Decorator'ı ---
def timeit(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        result = await func(*args, **kwargs)
        end_time = time.time()
        print(f"[SÜRE: {end_time - start_time:.2f} saniye] - Fonksiyon '{func.__name__}' tamamlandı.")
        return result
    return wrapper

# --- Collector Fonksiyonları ---
try:
    from app.collector import (
        fetch_live_events_via_page,
        fetch_all_event_details,
        fetch_player_matches,
        fetch_rankings_via_page,
        fetch_year_statistics,
        fetch_scheduled_events_for_dates
    )
except (ImportError, ModuleNotFoundError):
    print("UYARI: 'app.collector' bulunamadı. Lütfen script'in doğru dizinde olduğundan emin olun.")
    # Sahte (mock) fonksiyonlar
    async def fetch_live_events_via_page(*args, **kwargs): return {"events": []}
    async def fetch_all_event_details(*args, **kwargs): return [{"error": "mock"}]*3
    async def fetch_player_matches(team_id, page=0): return {"events": [], "hasNextPage": False}
    async def fetch_rankings_via_page(*args, **kwargs): return {"rankings": []}
    async def fetch_year_statistics(*args, **kwargs): return {"statistics": []}


# --- GÜNCELLENMİŞ Konfigürasyon ve Model Ağırlıkları ---
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
    # --- YENİ METRİKLER ---
    "servis_hakimiyeti": 0.10,
    "kritik_anlar_puani": 0.075,
    "hucum_puani": 0.075,
}
# Ağırlıkları normalize et
total_weight = sum(WEIGHTS.values())
if total_weight > 0:
    for k in WEIGHTS:
        WEIGHTS[k] /= total_weight

def print_debug(title: str, data: Any):
    print(f"\n--- {title} ---")
    try:
        print(json.dumps(data, indent=2, ensure_ascii=False))
    except TypeError:
        print("JSON formatına çevrilemeyen veri.")

def fractional_to_decimal(fractional: str) -> float:
    if "/" not in fractional:
        return float(fractional)
    try:
        num, den = map(int, fractional.split('/'))
        if den == 0: return 2.0
        return 1.0 + (num / den)
    except (ValueError, TypeError):
        return 2.0

@timeit
async def get_player_stats_for_years(team_id: int, years: List[int]) -> Dict[str, List]:
    tasks = [fetch_year_statistics(team_id, year) for year in years]
    results = await asyncio.gather(*tasks)
    all_yearly_stats = []
    for year_data in results:
        all_yearly_stats.extend(year_data.get("statistics", []))
    return {"all_stats": all_yearly_stats}

@timeit
async def get_event_details(event_id: int) -> Optional[Dict[str, Any]]:
    """
    Belirli bir maç ID'sinin temel bilgilerini, bugünün ve dünün planlanmış
    maçları arasından arayarak bulur.
    """
    print(f">>> {event_id} ID'li maçın temel bilgileri aranıyor...")
    
    # Bugünün ve dünün tarihini al
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    dates_to_check = [today.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")]
    
    # Planlanmış maçları collector'dan çek
    scheduled_events_data = await fetch_scheduled_events_for_dates(dates_to_check, headless=True)
    
    for event in scheduled_events_data.get("events", []):
        if event.get("id") == event_id:
            print(">>> Maç bulundu!")
            return {
                "home_team_id": event.get("homeTeam", {}).get("id"),
                "away_team_id": event.get("awayTeam", {}).get("id"),
                "home_team_name": event.get("homeTeam", {}).get("name"),
                "away_team_name": event.get("awayTeam", {}).get("name"),
                "ground_type": event.get("groundType"),
            }
            
    print(f">>> HATA: Son 2 günün maçları arasında {event_id} ID'li bir maç bulunamadı.")
    return None

@timeit
async def fetch_all_player_matches(team_id: int) -> Dict[str, Any]:
    print(f">> {team_id} ID'li oyuncu için tüm maç geçmişi çekiliyor...")
    all_events, page = [], 0
    while True:
        data = await fetch_player_matches(team_id, page=page)
        page_events = data.get("events", [])
        if not page_events: break
        all_events.extend(page_events)
        if not data.get("hasNextPage", False): break
        page += 1
    unique_events = {event['id']: event for event in all_events}.values()
    return {"events": sorted(list(unique_events), key=lambda x: x.get('startTimestamp', 0), reverse=True)}

@timeit
async def get_pre_match_data(event_id: int, home_team_id: int, away_team_id: int) -> Dict[str, Any]:
    print(f">>> Oyuncu ID'leri ({home_team_id}, {away_team_id}) için detaylı veri toplama işlemi başlatıldı...")
    current_year = datetime.now().year
    years_to_fetch = [current_year, current_year - 1, current_year - 2]
    tasks = {
        "home_rankings": fetch_rankings_via_page(home_team_id),
        "home_matches": fetch_all_player_matches(home_team_id),
        "home_yearly_stats": get_player_stats_for_years(home_team_id, years_to_fetch),
        "away_rankings": fetch_rankings_via_page(away_team_id),
        "away_matches": fetch_all_player_matches(away_team_id),
        "away_yearly_stats": get_player_stats_for_years(away_team_id, years_to_fetch),
    }
    results = await asyncio.gather(*tasks.values())
    data_map = dict(zip(tasks.keys(), results))
    home_data = {"rankings": data_map["home_rankings"], "matches": data_map["home_matches"], "yearly_stats": data_map["home_yearly_stats"]}
    away_data = {"rankings": data_map["away_rankings"], "matches": data_map["away_matches"], "yearly_stats": data_map["away_yearly_stats"]}
    vote_details = await fetch_all_event_details(event_id, ["votes", "odds/1/all"])
    match_details = dict(zip(["votes", "oddsAll"], vote_details))
    return {"match_details": match_details, "home_player": home_data, "away_player": away_data}

def calculate_metric_scores(data: Dict[str, Any], home_team_id: int, away_team_id: int, ground_type: str) -> tuple[Dict[str, float], Dict[str, float]]:
    # ... (Bu fonksiyonun içeriği aynı, değişiklik yok)
    home_scores, away_scores = {}, {}

    def aggregate_stats_for_surface(all_stats: List[Dict], surface: str) -> Dict:
        surface_stats = [s for s in all_stats if s.get("groundType") == surface]
        stats_to_aggregate = surface_stats if surface_stats else all_stats
        if not stats_to_aggregate:
            return defaultdict(float)
        aggregated = defaultdict(float)
        for stat_group in stats_to_aggregate:
            for key, value in stat_group.items():
                if isinstance(value, (int, float)):
                    aggregated[key] += value
        if not surface_stats:
            print(f"UYARI: '{surface}' yüzeyi için özel veri bulunamadı, genel istatistikler kullanılıyor.")
        return aggregated

    home_yearly_all = data['home_player']['yearly_stats']['all_stats']
    away_yearly_all = data['away_player']['yearly_stats']['all_stats']
    home_yearly = aggregate_stats_for_surface(home_yearly_all, ground_type)
    away_yearly = aggregate_stats_for_surface(away_yearly_all, ground_type)
    def calculate_score(home_val, away_val):
        total = home_val + away_val
        if total <= 0: return 0.5, 0.5
        return home_val / total, away_val / total

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
        pre_market = next(m for m in data['match_details']['oddsAll']['markets'] if not m.get('isLive') and m.get('marketName') == 'Full time')
        home_odds = fractional_to_decimal(next(c for c in pre_market['choices'] if c['name'] == '1').get('fractionalValue', "2.0"))
        away_odds = fractional_to_decimal(next(c for c in pre_market['choices'] if c['name'] == '2').get('fractionalValue', "2.0"))
        home_prob, away_prob = 1 / home_odds, 1 / away_odds
        home_scores['oran'], away_scores['oran'] = calculate_score(home_prob, away_prob)
    except Exception: home_scores['oran'], away_scores['oran'] = 0.5, 0.5
    try:
        votes = data['match_details']['votes']['vote']
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
    except Exception as e:
        home_scores['h2h'], away_scores['h2h'] = 0.5, 0.5
    def get_stats_from_matches(matches: List[Dict], player_id: int, player_name: str, limit: Optional[int] = None):
        if limit and len(matches) > limit: matches = matches[:limit]
        stats = defaultdict(float)
        player_last_name = player_name.split(' ')[-1].lower() if player_name else ''
        for event in matches:
            stats['total'] += 1
            winner_code = event.get('winnerCode')
            home_team_name, away_team_name = event.get('homeTeam', {}).get('name', '').lower(), event.get('awayTeam', {}).get('name', '').lower()
            is_home = (event.get('homeTeam', {}).get('id') == player_id) or (player_last_name and player_last_name in home_team_name)
            is_away = (event.get('awayTeam', {}).get('id') == player_id) or (player_last_name and player_last_name in away_team_name)
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

async def get_match_prediction(event_id: int):
    """
    Verilen bir maç ID'si için tam bir TGS analizi yapar ve sonucu döndürür.
    Bu fonksiyon, ana logic'i birleştirir ve dışarıdan çağrılabilir hale getirir.
    """
    overall_start_time = time.time()
    
    event_info = await get_event_details(event_id)
    if not event_info or not all(key in event_info for key in ["home_team_id", "away_team_id", "home_team_name", "away_team_name"]):
        return {"error": "Maçın temel bilgileri (oyuncu ID'leri vb.) alınamadı."}

    all_data = {**event_info, **await get_pre_match_data(event_id, event_info["home_team_id"], event_info["away_team_id"])}
    
    home_scores, away_scores = calculate_metric_scores(all_data, event_info["home_team_id"], event_info["away_team_id"], event_info["ground_type"])

    home_tgs = sum(WEIGHTS[key] * home_scores.get(key, 0.5) for key in WEIGHTS)
    away_tgs = sum(WEIGHTS[key] * away_scores.get(key, 0.5) for key in WEIGHTS)
    total_tgs = home_tgs + away_tgs
    home_prior = home_tgs / total_tgs if total_tgs > 0 else 0.5
    away_prior = away_tgs / total_tgs if total_tgs > 0 else 0.5

    print(f"\n[BİLGİ] {event_id} ID'li maç için TGS hesaplaması tamamlandı. Toplam süre: {time.time() - overall_start_time:.2f} saniye")

    return {
        "home_player_name": event_info["home_team_name"],
        "away_player_name": event_info["away_team_name"],
        "home_win_prob": home_prior,
        "away_win_prob": away_prior,
        "scores": {"home": home_scores, "away": away_scores},
        "weights": WEIGHTS
    }


async def main():
    # Test etmek için bir maç ID'si belirleyin (canlı, bitmiş veya gelecek olabilir)
    TEST_EVENT_ID = 14844202
    
    prediction_result = await get_match_prediction(TEST_EVENT_ID)

    if "error" in prediction_result:
        print(f"HATA: {prediction_result['error']}")
        return

    # --- Sonuçları yazdırma ---
    home_name = prediction_result["home_player_name"]
    away_name = prediction_result["away_player_name"]
    home_prior = prediction_result["home_win_prob"]
    away_prior = prediction_result["away_win_prob"]
    home_scores = prediction_result["scores"]["home"]
    away_scores = prediction_result["scores"]["away"]
    home_tgs = sum(WEIGHTS[key] * home_scores.get(key, 0.5) for key in WEIGHTS)
    away_tgs = sum(WEIGHTS[key] * away_scores.get(key, 0.5) for key in WEIGHTS)

    print("\n\n" + "="*80)
    print(" " * 25 + "MAÇ ÖNCESİ GELİŞMİŞ ANALİZ SONUÇLARI")
    print("="*80)
    print(f"{'Metrik (Ağırlık)':<28} | {'Ev Sahibi (' + home_name + ')':<25} | {'Deplasman (' + away_name + ')':<25}")
    print("-"*80)
    for key in sorted(WEIGHTS.keys()):
        if WEIGHTS[key] > 0:
            weight_percent = WEIGHTS[key] * 100
            metric_name_with_weight = f"{key.replace('_', ' ').capitalize()} ({weight_percent:.1f}%)"
            print(f"{metric_name_with_weight:<28} | {home_scores.get(key, 0.5):<25.3f} | {away_scores.get(key, 0.5):<25.3f}")
    print("-"*80)
    print(f"{'Ağırlıklı TGS':<28} | {home_tgs:<25.4f} | {away_tgs:<25.4f}")
    print("="*80)
    print("\n--- PRIOR OLASILIK (MAÇ ÖNCESİ KAZANMA İHTİMALİ) ---")
    print(f"Ev Sahibi ({home_name}) Kazanma Olasılığı: {home_prior:.2%}")
    print(f"Deplasman ({away_name}) Kazanma Olasılığı: {away_prior:.2%}")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(main())