#!/usr/bin/env python

import pandas as pd
import json
import glob
from pathlib import Path
import os

# Ścieżki do danych
PROJECT_DIR = Path(__file__).resolve().parents[2]  # Ścieżka do katalogu głównego projektu
RAW_DATA_DIR = PROJECT_DIR / "data" / "raw"
EVENTS_DIR = RAW_DATA_DIR / "events"
FRAMES_DIR = RAW_DATA_DIR / "frames360"


def load_matches():
    """
    Wczytuje dane o wszystkich meczach Bayeru Leverkusen.

    """
    matches_path = RAW_DATA_DIR / "leverkusen_matches.csv"
    if not os.path.exists(matches_path):
        raise FileNotFoundError(f"Plik z meczami nie istnieje: {matches_path}")
    
    return pd.read_csv(matches_path)


def load_events(match_id=None):
    """
    Wczytuje dane o wydarzeniach dla konkretnego meczu lub wszystkich meczów.

    """
    if match_id:
        # Wczytaj wydarzenia dla konkretnego meczu
        events_path = EVENTS_DIR / f"{match_id}.csv"
        if not os.path.exists(events_path):
            raise FileNotFoundError(f"Plik z wydarzeniami nie istnieje: {events_path}")
        
        events = pd.read_csv(events_path)
        events['match_id'] = match_id
    else:
        # Wczytaj wydarzenia ze wszystkich meczów
        all_event_files = glob.glob(str(EVENTS_DIR / "*.csv"))
        if not all_event_files:
            raise FileNotFoundError(f"Nie znaleziono plików z wydarzeniami w {EVENTS_DIR}")
        
        all_events = []
        for file_path in all_event_files:
            match_id = Path(file_path).stem
            try:
                events = pd.read_csv(file_path)
                events['match_id'] = match_id
                all_events.append(events)
            except Exception as e:
                print(f"Błąd podczas wczytywania pliku {file_path}: {e}")
        
        if not all_events:
            raise ValueError("Nie udało się wczytać żadnych danych o wydarzeniach")
        
        events = pd.concat(all_events, ignore_index=True)
    
    # Konwersja kolumn JSON na obiekty Pythona
    for col in ['type', 'location', 'shot', 'pass', 'carry', 'player', 'possession_team', 'team']:
        if col in events.columns:
            events[col] = events[col].apply(
                lambda x: json.loads(x) if isinstance(x, str) and pd.notna(x) and (x.startswith('{') or x.startswith('[')) else x
            )
    
    return events


def load_frames(match_id=None):
    """
    Wczytuje dane 360 dla konkretnego meczu lub wszystkich meczów.

    """
    if match_id:
        # Wczytaj dane 360 dla konkretnego meczu
        frames_path = FRAMES_DIR / f"{match_id}.csv"
        if not os.path.exists(frames_path):
            raise FileNotFoundError(f"Plik z danymi 360 nie istnieje: {frames_path}")
        
        frames = pd.read_csv(frames_path)
        frames['match_id'] = match_id
    else:
        # Wczytaj dane 360 ze wszystkich meczów
        all_frame_files = glob.glob(str(FRAMES_DIR / "*.csv"))
        if not all_frame_files:
            raise FileNotFoundError(f"Nie znaleziono plików z danymi 360 w {FRAMES_DIR}")
        
        all_frames = []
        for file_path in all_frame_files:
            match_id = Path(file_path).stem
            try:
                frames = pd.read_csv(file_path)
                frames['match_id'] = match_id
                all_frames.append(frames)
            except Exception as e:
                print(f"Błąd podczas wczytywania pliku {file_path}: {e}")
        
        if not all_frames:
            raise ValueError("Nie udało się wczytać żadnych danych 360")
        
        frames = pd.concat(all_frames, ignore_index=True)
    
    # Konwersja kolumn JSON na obiekty Pythona
    for col in ['visible_area', 'freeze_frame']:
        if col in frames.columns:
            frames[col] = frames[col].apply(
                lambda x: json.loads(x) if isinstance(x, str) and pd.notna(x) and (x.startswith('{') or x.startswith('[')) else x
            )
    
    # Zmiana nazwy event_uuid na id dla łatwiejszego łączenia z wydarzeniami
    if 'event_uuid' in frames.columns:
        frames = frames.rename(columns={'event_uuid': 'id'})
    
    return frames


def load_merged_data(match_id=None):
    """
    Wczytuje i łączy dane o wydarzeniach z danymi 360 dla konkretnego meczu lub wszystkich meczów.
    
    """
    events = load_events(match_id)
    
    try:
        frames = load_frames(match_id)
        
        # Upewnij się, że obie ramki danych mają wymagane kolumny do łączenia
        merge_cols = ['id']
        if match_id is None:
            merge_cols.append('match_id')
            
        merged_data = pd.merge(
            events,
            frames,
            how='left',
            on=merge_cols
        )
        
        # Dodaj informację, ile wydarzeń ma dane 360
        events_with_360 = merged_data['visible_area'].notna().sum()
        print(f"{events_with_360} wydarzeń ({events_with_360/len(merged_data)*100:.2f}%) ma dane 360")
        
        return merged_data
    except FileNotFoundError as e:
        print(f"Brak danych 360: {e}. Zwracam tylko dane o wydarzeniach.")
        return events


def events_filter(events_df, event_type=None, player_name=None):
    """
    Filtruje wydarzenia według typu lub zawodnika.

    """
    filtered_df = events_df.copy()
    
    # Filtrowanie po typie wydarzenia
    if event_type:
        filtered_df = filtered_df[filtered_df['type'].apply(
            lambda x: x.get('name') == event_type if isinstance(x, dict) and 'name' in x else False
        )]
    
    # Filtrowanie po zawodniku
    if player_name:
        filtered_df = filtered_df[filtered_df['player'].apply(
            lambda x: x.get('name') == player_name if isinstance(x, dict) and 'name' in x else False
        )]
    
    return filtered_df