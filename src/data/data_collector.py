#!/usr/bin/env python
import pandas as pd
import numpy as np
from statsbombpy import sb
import warnings
import json
from pathlib import Path

# Ignoruj ostrzeżenia o braku uwierzytelnienia
warnings.filterwarnings('ignore', category=UserWarning)

# Ustalenie ścieżek zapisu
PROJECT_DIR = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_DIR / "data" / "raw"
EVENTS_DIR = RAW_DATA_DIR / "events"
FRAMES_DIR = RAW_DATA_DIR / "frames360"

# Tworzenie katalogów, jeśli nie istnieją
EVENTS_DIR.mkdir(parents=True, exist_ok=True)
FRAMES_DIR.mkdir(parents=True, exist_ok=True)

def condense_frame_data(frames_df):
    """
    Kondensuje dane 360, grupując je tak, aby jeden wiersz 
    odpowiadał jednemu wydarzeniu zamiast jednemu zawodnikowi.
    """
    # Grupuj po id i match_id
    grouped = frames_df.groupby(['id', 'match_id'])
    
    # Tworzymy nowy DataFrame dla skondensowanych danych
    result = []
    for (event_id, match_id), group in grouped:
        # Pobierz pierwszą wartość visible_area (taka sama dla wszystkich wierszy grupy)
        visible_area = group['visible_area'].iloc[0]
        
        # Utwórz listę zawodników
        freeze_frame = []
        for _, row in group.iterrows():
            player_location = json.loads(row['location']) if isinstance(row['location'], str) else row['location']
            player_data = {
                'location': player_location,
                'teammate': row['teammate'],
                'actor': row['actor'],
                'keeper': row['keeper']
            }
            freeze_frame.append(player_data)
        
        # Dodaj wiersz do wyników
        result.append({
            'id': event_id,
            'match_id': match_id,
            'visible_area': visible_area,
            'freeze_frame': freeze_frame  # Bezpośrednio używamy listy, nie konwertujemy do JSON
        })
    
    # Utwórz DataFrame z wynikami
    return pd.DataFrame(result)

# Pobieranie wszystkich meczów Bundesligi z sezonu 2023/24
matches = sb.matches(competition_id=9, season_id=281)

# Filtrowane tylko mecze Bayeru Leverkusen
leverkusen_matches = matches[
    (matches['home_team'] == "Bayer Leverkusen") | 
    (matches['away_team'] == "Bayer Leverkusen")
]

# Utworzenie listy meczów
leverkusen_matches.to_csv(RAW_DATA_DIR / "leverkusen_matches.csv", index=False)
leverkusen_matches.to_parquet(RAW_DATA_DIR / "leverkusen_matches.parquet", index=False)
print(f"Zapisano listę {len(leverkusen_matches)} meczów Bayeru Leverkusen w formatach CSV i Parquet")

# Dla każdego meczu pobierz dane o wydarzeniach
for match_id in leverkusen_matches['match_id']:
    print(f"Przetwarzanie meczu {match_id}...")
    
    # Pobierz dane o wydarzeniach
    events = sb.events(match_id=match_id)
    # Zapisz wydarzenia w formatach CSV i Parquet
    events.to_csv(EVENTS_DIR / f"{match_id}.csv", index=False)
    events.to_parquet(EVENTS_DIR / f"{match_id}.parquet", index=False)
    print(f"  Zapisano {len(events)} wydarzeń w formatach CSV i Parquet")
    
    # Pobierz dane 360
    try:
        frames = sb.frames(match_id=match_id)
        
        # Jeśli pobrano dane 360, kondensuj je
        if not frames.empty:
            condensed_frames = condense_frame_data(frames)
            condensed_frames.to_csv(FRAMES_DIR / f"{match_id}.csv", index=False)
            condensed_frames.to_parquet(FRAMES_DIR / f"{match_id}.parquet", index=False)
            print(f"  Zapisano dane 360 w formatach CSV i Parquet")
        else:
            print(f"  Brak danych 360 dla meczu {match_id}")
    except Exception as e:
        print(f"  Błąd podczas pobierania danych 360: {e}")

print("Zakończono pobieranie i przetwarzanie danych.")