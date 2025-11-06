import pandas as pd
import sqlite3
import os
import time
import requests
import random
import re
import json
from dotenv import load_dotenv
from rapidfuzz import fuzz, process


# ------------------------------
# ENVIRONMENT VARIABLES
# ------------------------------
load_dotenv()
DB_PATH = "movies.db"
MOVIES_CSV = "data/movies.csv"
RATINGS_CSV = "data/ratings.csv"
OMDB_API_KEY = os.getenv("OMDB_API_KEY")

# Delete existing database
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)
    print("Existing database deleted.")

# ------------------------------
# CONFIGURATION
# ------------------------------
CACHE_FILE = "omdb_cache.json"
BATCH_SIZE = 200   # For large datasets
SLEEP_TIME = 0.3   # Delay between API calls
TEST_MODE = True
TEST_LIMIT = 500   # For test runs; set False to process all records

# ------------------------------
# DATABASE CREATION
# ------------------------------
def create_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    with open("schema.sql", "r") as f:
        cursor.executescript(f.read())
    conn.commit()
    conn.close()
    print("Database and tables ready.")

# ------------------------------
# TITLE CLEANING
# ------------------------------
OMDB_MANUAL_TITLES = {
    "Lawnmower Man 2: Beyond Cyberspace": "Lawnmower Man 2",
    "In the Bleak Midwinter": "A Midwinter's Tale",
    "Misérables, Les": "Les Misérables"
}

def clean_title(title: str) -> str:
    title = re.sub(r"\(a\.k\.a\..*?\)", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\(.*?\)", "", title)
    return title.strip()

def normalize_title(title: str) -> str:
    if ", The" in title: title = "The " + title.replace(", The", "")
    elif ", A" in title: title = "A " + title.replace(", A", "")
    elif ", An" in title: title = "An " + title.replace(", An", "")
    return title.strip()

# ------------------------------
# ETL FUNCTIONS
# ------------------------------
def extract_data():
    movies_df = pd.read_csv(MOVIES_CSV, encoding="utf-8")
    ratings_df = pd.read_csv(RATINGS_CSV, encoding="utf-8")

    if TEST_MODE:
        movies_df = movies_df.head(TEST_LIMIT)
        ratings_df = ratings_df[ratings_df['movieId'].isin(movies_df['movieId'])]
        print(f"Test mode: Processing first {TEST_LIMIT} movies and their ratings.")
    else:
        print(f"Processing all {len(movies_df)} movies and {len(ratings_df)} ratings.")

    return movies_df, ratings_df

def transform_and_enrich(movies_df, ratings_df):
    movies_df["genres_list"] = movies_df["genres"].apply(lambda x: x.split("|") if pd.notnull(x) else [])
    movies_df["year"] = movies_df["title"].str.extract(r"\((\d{4})\)").astype(float)

    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
    else:
        cache = {}

    directors, plots, box_offices = [], [], []

    for idx, row in movies_df.iterrows():
        title = normalize_title(clean_title(row["title"]))
        title = OMDB_MANUAL_TITLES.get(title, title)
        year = int(row["year"]) if not pd.isna(row["year"]) else None

        # Use cache if available
        if title in cache and cache[title].get("Response") == "True":
            data = cache[title]
        else:
            retries = 3
            data = {"Response": "False"}
            for attempt in range(retries):
                try:
                    # Exact search
                    params = {"t": title, "apikey": OMDB_API_KEY}
                    if year: params["y"] = year
                    response = requests.get("https://www.omdbapi.com/", params=params, timeout=5)
                    response.raise_for_status()
                    data = response.json()

                    # Fuzzy search if exact fails
                    if data.get("Response") == "False":
                        params_search = {"s": title, "apikey": OMDB_API_KEY}
                        if year: params_search["y"] = year
                        search_resp = requests.get("https://www.omdbapi.com/", params=params_search, timeout=5)
                        search_resp.raise_for_status()
                        search_data = search_resp.json()

                        if search_data.get("Response") == "True":
                            candidates = search_data.get("Search", [])
                            candidate_titles = [m["Title"] for m in candidates]
                            # Use token_sort_ratio for better fuzzy match (handles word reordering)
                            best_match = process.extractOne(title, candidate_titles, scorer=fuzz.token_sort_ratio)
    
                            if best_match and best_match[1] > 80:  # accept only strong matches
                                matched_title = best_match[0]
                                imdb_id = next(m["imdbID"] for m in candidates if m["Title"] == matched_title)
                                print(f"🔍 Fuzzy match found: {matched_title} ({best_match[1]}%)")
        
                                # Fetch full movie details
                                details_resp = requests.get(
                                    "https://www.omdbapi.com/",
                                    params={"i": imdb_id, "apikey": OMDB_API_KEY},
                                    timeout=5
                                )
                                details_resp.raise_for_status()
                                data = details_resp.json()


                    cache[title] = data
                    break

                except Exception as e:
                    if attempt < retries - 1:
                        wait = 2 ** attempt + random.random()
                        print(f"Retry {attempt+1} for {title} in {wait:.2f}s due to error: {e}")
                        time.sleep(wait)
                    else:
                        print(f"Failed to fetch OMDb data for {title}: {e}")
                        data = {"Response": "False"}
                        cache[title] = data

            time.sleep(SLEEP_TIME)

        directors.append(data.get("Director", "N/A") if data.get("Response")=="True" else "N/A")
        plots.append(data.get("Plot", "N/A") if data.get("Response")=="True" else "N/A")
        box_offices.append(data.get("BoxOffice", "N/A") if data.get("Response")=="True" else "N/A")

        if idx % 50 == 0:
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(cache, f, indent=2)
            print(f"Progress: {idx}/{len(movies_df)} movies cached...")

    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)

    movies_df["director"] = directors
    movies_df["plot"] = plots
    movies_df["box_office"] = box_offices
    movies_df["decade"] = (movies_df["year"] // 10 * 10).astype("Int64")
    ratings_df["timestamp"] = pd.to_datetime(ratings_df["timestamp"], unit="s")
    return movies_df, ratings_df

def load_data(movies_df, ratings_df):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Load genres
    all_genres = set(g for sublist in movies_df["genres_list"] for g in sublist)
    for genre in all_genres:
        cursor.execute("INSERT OR IGNORE INTO genres (name) VALUES (?)", (genre,))
    conn.commit()

    # Map genres
    genre_map = {name: genre_id for genre_id, name in cursor.execute("SELECT genre_id, name FROM genres")}

    # Load movies
    for _, row in movies_df.iterrows():
        cursor.execute("""
            INSERT OR IGNORE INTO movies (movie_id, title, year, director, plot, box_office)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (row["movieId"], row["title"], row["year"], row["director"], row["plot"], row["box_office"]))
    conn.commit()

    # Load movie_genres
    for _, row in movies_df.iterrows():
        for genre in row["genres_list"]:
            genre_id = genre_map.get(genre)
            cursor.execute("INSERT OR IGNORE INTO movie_genres (movie_id, genre_id) VALUES (?, ?)",
                           (row["movieId"], genre_id))
    conn.commit()

    # Load ratings
    for _, row in ratings_df.iterrows():
        cursor.execute("""
            INSERT OR IGNORE INTO ratings (user_id, movie_id, rating, timestamp)
            VALUES (?, ?, ?, ?)
        """, (row["userId"], row["movieId"], row["rating"], row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit()
    conn.close()
    print("Data loaded into database successfully.")

# ------------------------------
# MAIN
# ------------------------------
if __name__ == "__main__":
    create_db()
    movies_df, ratings_df = extract_data()
    movies_df, ratings_df = transform_and_enrich(movies_df, ratings_df)
    load_data(movies_df, ratings_df)
