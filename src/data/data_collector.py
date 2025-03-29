#!/usr/bin/env python

import pandas as pd
from statsbombpy import sb
import warnings
warnings.filterwarnings('ignore', category=UserWarning)
from pathlib import Path

# Ustalenie ścieżek zapisu
PROJECT_DIR = Path(__file__).resolve().parents[2]

RAW_DATA_DIR = PROJECT_DIR / "data" / "raw"
EVENTS_DIR = RAW_DATA_DIR / "events"
FRAMES_DIR = RAW_DATA_DIR / "frames360"

# Tworzenie katalogów, jeśli nie istnieją
EVENTS_DIR.mkdir(parents=True, exist_ok=True)
FRAMES_DIR.mkdir(parents=True, exist_ok=True)

# Pobieranie wszystkiych meczów Bundesligi z sezonu 2023/24
matches = sb.matches(competition_id=9, season_id=281)

# Filtrowane tylko mecze Bayeru Leverkusen
leverkusen_matches = matches[
    (matches['home_team'] == "Bayer Leverkusen") | 
    (matches['away_team'] == "Bayer Leverkusen")
]

# Utworzenie listy meczów
leverkusen_matches.to_csv(RAW_DATA_DIR / "leverkusen_matches.csv", index=False)

# Dla każdego meczu pobierz dane o wydarzeniach
for match_id in leverkusen_matches['match_id']:
    # Pobierz dane o wydarzeniach
    events = sb.events(match_id=match_id)
    events.to_csv(EVENTS_DIR / f"{match_id}.csv", index=False)
    
    # Pobierz dane 360
    frames = sb.frames(match_id=match_id)
    frames.to_csv(FRAMES_DIR / f"{match_id}.csv", index=False)