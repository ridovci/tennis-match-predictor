# C:\Users\admin\Desktop\tenis\tennis-match-predictor-main\Tennis\app\create_dataset.py

import sys
import asyncio
import json
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
from pathlib import Path

# Absolute import'lar
try:
    from collector import (
        fetch_scheduled_events_for_dates,
        fetch_all_event_details,
        fetch_player_profile,
        fetch_rankings_via_page,
        fetch_player_matches,
        fetch_year_statistics
    )
    from tgs_calculator import fractional_to_decimal
except ImportError as e:
    print(f"HATA: Gerekli modüller yüklenemedi. 'create_dataset.py' dosyasının 'app' klasörü içinde olduğundan emin olun. Hata: {e}")
    exit()

# (Dosyanın geri kalanı öncekiyle aynı, herhangi bir değişiklik yok)
# ... [ Geri kalan tüm kod buraya gelecek ] ...
# ...
# Hata loglaması için temel yapılandırma
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- ANA VERİ TOPLAMA FONKSİYONLARI ---

async def get_pre_match_features(event_id, home_team_id, away_team_id):
    """
    Maç öncesi bilinen verileri (sıralamalar, oranlar, H2H vb.) çeker.
    """
    endpoints = ["votes", "odds/1/all", "h2h"]
    details_list = await fetch_all_event_details(event_id, endpoints)
    match_details = dict(zip(["votes", "oddsAll", "h2h"], details_list))

    tasks = {
        "home_rankings": fetch_rankings_via_page(home_team_id),
        "away_rankings": fetch_rankings_via_page(away_team_id),
        "home_matches": fetch_player_matches(home_team_id),
        "away_matches": fetch_player_matches(away_team_id),
        "home_profile": fetch_player_profile(home_team_id),
        "away_profile": fetch_player_profile(away_team_id)
    }
    results = await asyncio.gather(*tasks.values())
    data_map = dict(zip(tasks.keys(), results))

    return {
        "match_details": match_details,
        "home_rankings": data_map["home_rankings"],
        "away_rankings": data_map["away_rankings"],
        "home_matches": data_map["home_matches"],
        "away_matches": data_map["away_matches"],
        "home_profile": data_map["home_profile"],
        "away_profile": data_map["away_profile"]
    }

async def get_in_match_statistics(event_id):
    """
    Maç bittikten sonra oluşan detaylı istatistikleri çeker.
    """
    results = await fetch_all_event_details(event_id, ["statistics"])
    return results[0] if results else None

def flatten_data_to_row(event, pre_match_data, in_match_stats):
    """
    Toplanan tüm verileri tek bir sözlük (satır) haline getirir.
    """
    row = {
        'event_id': event.get('id'),
        'match_date': datetime.fromtimestamp(event.get('startTimestamp', 0)).strftime('%Y-%m-%d'),
        'tournament_name': event.get('tournament', {}).get('name'),
        'ground_type': event.get('groundType'),
        'home_player_id': event.get('homeTeam', {}).get('id'),
        'home_player_name': event.get('homeTeam', {}).get('name'),
        'away_player_id': event.get('awayTeam', {}).get('id'),
        'away_player_name': event.get('awayTeam', {}).get('name'),
        'winner': 1 if event.get('winnerCode') == 1 else 0,
    }

    try:
        home_ranks = pre_match_data.get('home_rankings', {}).get('rankings', [])
        away_ranks = pre_match_data.get('away_rankings', {}).get('rankings', [])
        row['home_rank'] = next((r['ranking'] for r in home_ranks if r.get('rankingClass') == 'team'), None)
        row['away_rank'] = next((r['ranking'] for r in away_ranks if r.get('rankingClass') == 'team'), None)
        row['rank_diff'] = (row['away_rank'] - row['home_rank']) if row['home_rank'] and row['away_rank'] else None
    except Exception: pass

    try:
        odds_markets = pre_match_data.get('match_details', {}).get('oddsAll', {}).get('markets', [])
        pre_market = next(m for m in odds_markets if not m.get('isLive') and m.get('marketName') == 'Full time')
        row['pre_match_home_odds'] = fractional_to_decimal(next(c['fractionalValue'] for c in pre_market['choices'] if c['name'] == '1'))
        row['pre_match_away_odds'] = fractional_to_decimal(next(c['fractionalValue'] for c in pre_market['choices'] if c['name'] == '2'))
    except Exception: pass

    try:
        h2h = pre_match_data.get('match_details', {}).get('h2h', {}).get('teamDuel', {})
        row['h2h_home_wins'] = h2h.get('homeWins')
        row['h2h_away_wins'] = h2h.get('awayWins')
    except Exception: pass

    if in_match_stats and 'statistics' in in_match_stats:
        stats = in_match_stats['statistics']
        if stats and stats[0].get('groups'):
            for group in stats[0]['groups']:
                for item in group['statisticsItems']:
                    stat_name = item['name'].lower().replace(' ', '_').replace('(', '').replace(')', '').replace('%', '_percentage')
                    row[f'home_{stat_name}'] = item.get('home')
                    row[f'away_{stat_name}'] = item.get('away')

    return row

# --- ANA İŞ AKIŞI ---

async def main():
    # Test için kısa tarih aralığı
    START_DATE = datetime.now() - timedelta(days=2)
    END_DATE = datetime.now() - timedelta(days=1)
    
    logging.info(f"Veri toplama işlemi başlatıldı. Tarih aralığı: {START_DATE.strftime('%Y-%m-%d')} -> {END_DATE.strftime('%Y-%m-%d')}")

    all_match_data = []
    
    current_date = START_DATE
    while current_date <= END_DATE:
        date_str = current_date.strftime('%Y-%m-%d')
        logging.info(f"'{date_str}' tarihi için biten maçlar taranıyor...")

        try:
            scheduled_data = await fetch_scheduled_events_for_dates([date_str])
            finished_events = [e for e in scheduled_data.get('events', []) if e.get('status', {}).get('type') == 'finished' and e.get('winnerCode')]
            
            
            logging.info(f"'{date_str}' tarihinde {len(finished_events)} adet bitmiş maç bulundu.")

            # Sadece ilk 10 maçı işle
            for event in finished_events[:2]:
                event_id, home_id, away_id = event.get('id'), event.get('homeTeam', {}).get('id'), event.get('awayTeam', {}).get('id')

                if not all([event_id, home_id, away_id]):
                    logging.warning(f"Eksik ID bilgisi: event={event_id}, home={home_id}, away={away_id}. Bu maç atlanıyor.")
                    continue

                try:
                    logging.info(f"İşleniyor: Maç ID {event_id} ({event.get('homeTeam',{}).get('name')} vs {event.get('awayTeam',{}).get('name')})")
                    pre_match_data = await get_pre_match_features(event_id, home_id, away_id)
                    in_match_stats = await get_in_match_statistics(event_id)
                    match_row = flatten_data_to_row(event, pre_match_data, in_match_stats)
                    all_match_data.append(match_row)
                    await asyncio.sleep(2) 
                except Exception as e:
                    logging.error(f"Maç ID {event_id} işlenirken bir hata oluştu: {e}", exc_info=False) # exc_info=False daha temiz bir log için
        except Exception as e:
            logging.error(f"'{date_str}' tarihi işlenirken genel bir hata oluştu: {e}", exc_info=False)

        current_date += timedelta(days=1)

    if not all_match_data:
        logging.warning("Hiçbir maç verisi toplanamadı.")
        return

    logging.info(f"Toplam {len(all_match_data)} maç verisi toplandı. Dosyalar kaydediliyor...")
    
    df = pd.DataFrame(all_match_data)
    output_dir = Path(__file__).resolve().parent.parent # 'app' klasörünün bir üstü, yani 'Tennis'
    
    csv_filename = output_dir / "tennis_ml_dataset.csv"
    df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
    logging.info(f"Veri seti başarıyla '{csv_filename}' dosyasına kaydedildi.")
    
    json_filename = output_dir / "tennis_ml_dataset.json"
    df.to_json(json_filename, orient='records', indent=4, force_ascii=False)
    logging.info(f"Veri seti başarıyla '{json_filename}' dosyasına kaydedildi.")


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    logging.info(f"Tüm işlem { (end_time - start_time) / 60:.2f} dakikada tamamlandı.")