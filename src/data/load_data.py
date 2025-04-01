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
    matches_path = RAW_DATA_DIR / "leverkusen_matches.parquet"
    if not matches_path.exists():
        raise FileNotFoundError(f"Plik z meczami nie istnieje: {matches_path}")
    
    return pd.read_parquet(matches_path)


def load_events(match_id=None):
    """
    Wczytuje dane o wydarzeniach dla konkretnego meczu lub wszystkich meczów.
    """
    if match_id:
        # Wczytaj wydarzenia dla konkretnego meczu
        events_path = EVENTS_DIR / f"{match_id}.parquet"
        if not events_path.exists():
            raise FileNotFoundError(f"Plik z wydarzeniami nie istnieje: {events_path}")
        
        events = pd.read_parquet(events_path)
        events['match_id'] = match_id
    else:
        # Wczytaj wydarzenia ze wszystkich meczów
        all_event_files = list(EVENTS_DIR.glob("*.parquet"))
        if not all_event_files:
            raise FileNotFoundError(f"Nie znaleziono plików z wydarzeniami w {EVENTS_DIR}")
        
        all_events = []
        for file_path in all_event_files:
            match_id = file_path.stem
            try:
                events = pd.read_parquet(file_path)
                events['match_id'] = match_id
                all_events.append(events)
            except Exception as e:
                print(f"Błąd podczas wczytywania pliku {file_path}: {e}")
        
        if not all_events:
            raise ValueError("Nie udało się wczytać żadnych danych o wydarzeniach")
        
        events = pd.concat(all_events, ignore_index=True)
    
    return events


def load_frames(match_id=None):
    """
    Wczytuje dane 360 dla konkretnego meczu lub wszystkich meczów.
    """
    if match_id:
        # Wczytaj dane 360 dla konkretnego meczu
        frames_path = FRAMES_DIR / f"{match_id}.parquet"
        if not frames_path.exists():
            raise FileNotFoundError(f"Plik z danymi 360 nie istnieje: {frames_path}")
        
        frames = pd.read_parquet(frames_path)
        frames['match_id'] = match_id
    else:
        # Wczytaj dane 360 ze wszystkich meczów
        all_frame_files = list(FRAMES_DIR.glob("*.parquet"))
        if not all_frame_files:
            raise FileNotFoundError(f"Nie znaleziono plików z danymi 360 w {FRAMES_DIR}")
        
        all_frames = []
        for file_path in all_frame_files:
            match_id = file_path.stem
            try:
                frames = pd.read_parquet(file_path)
                frames['match_id'] = match_id
                all_frames.append(frames)
            except Exception as e:
                print(f"Błąd podczas wczytywania pliku {file_path}: {e}")
        
        if not all_frames:
            raise ValueError("Nie udało się wczytać żadnych danych 360")
        
        frames = pd.concat(all_frames, ignore_index=True)
    
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
        filtered_df = filtered_df[filtered_df['type'].str.lower() == event_type.lower()]

    # Filtrowanie po zawodniku
    if player_name:
        filtered_df = filtered_df[filtered_df['player'].str.lower() == player_name.lower()]
    
    return filtered_df

