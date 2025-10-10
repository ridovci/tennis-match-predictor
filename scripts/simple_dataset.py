# Basit Tenis Veri Toplama - Sadece 2 Maç için Hızlı Test

import sys
import asyncio
import pandas as pd
from datetime import datetime, timedelta
import time
import logging
from pathlib import Path

# Proje kök dizinini Python path'ine ekle
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

try:
    from app.collector import fetch_scheduled_events_for_dates, fetch_all_event_details
except ImportError as e:
    print(f"HATA: {e}")
    exit()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

async def get_simple_match_data(event):
    """Sadece temel maç verilerini alır - çok hızlı"""
    event_id = event.get('id')
    home_team = event.get('homeTeam', {})
    away_team = event.get('awayTeam', {})
    
    # Temel bilgiler
    row = {
        'event_id': event_id,
        'match_date': datetime.fromtimestamp(event.get('startTimestamp', 0)).strftime('%Y-%m-%d'),
        'tournament_name': event.get('tournament', {}).get('name', ''),
        'ground_type': event.get('groundType', ''),
        'home_player_id': home_team.get('id'),
        'home_player_name': home_team.get('name', ''),
        'away_player_id': away_team.get('id'),
        'away_player_name': away_team.get('name', ''),
        'winner': 1 if event.get('winnerCode') == 1 else 0,
    }
    
    # Sadece temel endpoint'leri çek (hızlı)
    try:
        logger.info(f"Maç {event_id} için temel veriler çekiliyor...")
        endpoints = ["votes", "odds/1/all"]
        results = await fetch_all_event_details(event_id, endpoints)
        
        # Oranlar
        if results[1] and 'markets' in results[1]:
            markets = results[1]['markets']
            pre_market = next((m for m in markets if not m.get('isLive') and m.get('marketName') == 'Full time'), None)
            if pre_market and 'choices' in pre_market:
                home_choice = next((c for c in pre_market['choices'] if c['name'] == '1'), None)
                away_choice = next((c for c in pre_market['choices'] if c['name'] == '2'), None)
                if home_choice and away_choice:
                    row['home_odds'] = home_choice.get('decimalValue', 2.0)
                    row['away_odds'] = away_choice.get('decimalValue', 2.0)
        
        # Oylar
        if results[0] and 'vote' in results[0]:
            vote = results[0]['vote']
            vote1 = vote.get('vote1', 0)
            vote2 = vote.get('vote2', 0)
            total = vote1 + vote2
            if total > 0:
                row['home_vote_percentage'] = vote1 / total
                row['away_vote_percentage'] = vote2 / total
        
        logger.info(f"Maç {event_id} tamamlandı!")
        return row
        
    except Exception as e:
        logger.warning(f"Maç {event_id} için hata: {e}")
        return row

async def main():
    logger.info("=== BASİT TENİS VERİ TOPLAMA BAŞLATILDI ===")
    
    # Sadece bugünün verilerini al
    today = datetime.now().date()
    date_str = today.strftime('%Y-%m-%d')
    
    logger.info(f"'{date_str}' tarihi için maçlar taranıyor...")
    
    try:
        # Planlanan maçları çek
        scheduled_data = await fetch_scheduled_events_for_dates([date_str])
        events = scheduled_data.get('events', [])
        
        # Sadece bitmiş maçları al
        finished_events = [
            e for e in events 
            if e.get('status', {}).get('type') == 'finished' 
            and e.get('winnerCode') in [1, 2]
        ]
        
        logger.info(f"'{date_str}' tarihinde {len(finished_events)} adet bitmiş maç bulundu.")
        
        # Sadece ilk 2 maçı al
        test_events = finished_events[:2]
        logger.info(f"Sadece ilk {len(test_events)} maç işlenecek")
        
        # Maçları işle
        all_data = []
        for i, event in enumerate(test_events, 1):
            logger.info(f"İşleniyor: {i}/2 - {event.get('homeTeam',{}).get('name')} vs {event.get('awayTeam',{}).get('name')}")
            match_data = await get_simple_match_data(event)
            all_data.append(match_data)
            await asyncio.sleep(1)  # 1 saniye bekle
        
        # CSV'ye kaydet
        if all_data:
            df = pd.DataFrame(all_data)
            output_file = Path(__file__).resolve().parent.parent / "simple_tennis_dataset.csv"
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"✅ {len(all_data)} maç verisi '{output_file}' dosyasına kaydedildi!")
            logger.info(f"📊 Sütunlar: {list(df.columns)}")
        else:
            logger.warning("Hiç veri toplanamadı!")
            
    except Exception as e:
        logger.error(f"Genel hata: {e}")

if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    end_time = time.time()
    logger.info(f"⏱️ Toplam süre: {end_time - start_time:.2f} saniye")
