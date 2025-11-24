#!/usr/bin/env python3
"""
etl_pg.py

Idempotent ETL for MovieLens dataset,
using PostgreSQL instead of SQLite,
with full verbose logging.

"""

import argparse
import json
import os
import re
import time
from pathlib import Path

import pandas as pd
import requests
import psycopg2
import psycopg2.extras

OMDB_CACHE_FILE = "omdb_cache.json"

MOVIE_YEAR_RE = re.compile(r"\((\d{4})\)")


def load_cache(path: str):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                c = json.load(f)
                return c
        except Exception:
            print("Cache load failed. Using empty cache")
            return {}
    print("No cache found. Starting empty")
    return {}


def save_cache(path: str, cache: dict):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
    print("Cache saved")


def parse_year_from_title(title: str):
    m = MOVIE_YEAR_RE.search(title)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def clean_title(title: str):
    return MOVIE_YEAR_RE.sub("", title).strip()


def query_omdb(title: str, year: int, apikey: str, session: requests.Session):
    base = "http://www.omdbapi.com/"
    params = {"apikey": apikey, "t": title}
    if year:
        params["y"] = str(year)

    resp = session.get(base, params=params, timeout=10)
    data = resp.json()
    if data.get("Response") == "True":
        print("OMDb success")
        return data

    print("OMDb no response WITH year. Trying again WITHOUT year")
    if "y" in params:
        params.pop("y")
        resp = session.get(base, params=params, timeout=10)
        data = resp.json()
        if data.get("Response") == "True":
            print("OMDb success after removing year")
            return data

    print("OMDb lookup failed")
    return None


def ensure_db_schema(conn, schema_sql: str):
    cur = conn.cursor()
    cur.execute(schema_sql)
    conn.commit()
    cur.close()
    print("Schema ensured")


def upsert_movie(conn, movie):
    sql = '''
    INSERT INTO movies(movie_id, title, year, director, plot, box_office, imdb_id, omdb_raw)
    VALUES(%(movie_id)s, %(title)s, %(year)s, %(director)s, %(plot)s, %(box_office)s, %(imdb_id)s, %(omdb_raw)s)
    ON CONFLICT (movie_id)
    DO UPDATE SET
      title = EXCLUDED.title,
      year = EXCLUDED.year,
      director = EXCLUDED.director,
      plot = EXCLUDED.plot,
      box_office = EXCLUDED.box_office,
      imdb_id = EXCLUDED.imdb_id,
      omdb_raw = EXCLUDED.omdb_raw
    '''
    with conn.cursor() as cur:
        cur.execute(sql, movie)
    conn.commit()
    print("UPSERT successful")


def get_genre_id(conn, name):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM genres WHERE name = %s", (name,))
        r = cur.fetchone()
        if r:
            return r[0]

        print("Genre not found, inserting")
        cur.execute("INSERT INTO genres(name) VALUES(%s) RETURNING id", (name,))
        gid = cur.fetchone()[0]
    conn.commit()
    return gid


def upsert_movie_genres(conn, movie_id, genre_list):
    for g in genre_list:
        gid = get_genre_id(conn, g)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO movie_genres(movie_id, genre_id) VALUES(%s,%s)",
                    (movie_id, gid),
                )
            conn.commit()
            print(f"Attached genre {g}")
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            print(f"Genre {g} already attached, skipping")


def load_ratings(conn, df):
    df = df.rename(columns={"movieId": "movie_id", "userId": "user_id"})
    rows = df[["user_id", "movie_id", "rating", "timestamp"]].to_dict(orient="records")

    for r in rows:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO ratings(user_id, movie_id, rating, timestamp) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (
                        r["user_id"],
                        r["movie_id"],
                        float(r["rating"]),
                        int(r.get("timestamp") or 0)
                    ),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            print("Rating insert failed, skipped")

    print("All ratings loaded")


def main(args):

    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(args.db)

    # Load schema.sql
    if args.schema and os.path.exists(args.schema):
        schema_sql = Path(args.schema).read_text()
    else:
        raise SystemExit("Missing schema.sql")

    ensure_db_schema(conn, schema_sql)

    print("Reading movies CSV...")
    movies_df = pd.read_csv(args.movies)
    print("Reading ratings CSV...")
    ratings_df = pd.read_csv(args.ratings)

    movies_df = movies_df.dropna(subset=["movieId"])

    cache = load_cache(OMDB_CACHE_FILE)
    session = requests.Session()

    for _, row in movies_df.iterrows():

        try:
            movie_id = int(row["movieId"])
        except Exception:
            print("Invalid movieId")
            continue

        raw_title = str(row.get("title") or "").strip()
        year = parse_year_from_title(raw_title)
        title_for_query = clean_title(raw_title)


        genres_raw = str(row.get("genres") or "")
        genre_list = [g for g in genres_raw.split("|") if g and g != "(no genres listed)"]

        omdb_key = args.apikey or os.environ.get("OMDB_API_KEY")

        omdb_data = None
        cache_key = f"{title_for_query}{year}"

        if cache_key in cache:
            omdb_data = cache[cache_key]
        else:
            if omdb_key:
                try:
                    omdb_resp = query_omdb(title_for_query, year, omdb_key, session)
                    if omdb_resp:
                        omdb_data = omdb_resp
                        cache[cache_key] = omdb_data
                        time.sleep(0.2)
                    else:
                        cache[cache_key] = None
                except Exception:
                    print("OMDb exception")
                    cache[cache_key] = None

        movie_record = {
            "movie_id": movie_id,
            "title": raw_title,
            "year": year,
            "director": None,
            "plot": None,
            "box_office": None,
            "imdb_id": None,
            "omdb_raw": None,
        }

        if omdb_data:
            movie_record.update(
                {
                    "director": omdb_data.get("Director"),
                    "plot": omdb_data.get("Plot"),
                    "box_office": omdb_data.get("BoxOffice"),
                    "imdb_id": omdb_data.get("imdbID"),
                    "omdb_raw": json.dumps(omdb_data, ensure_ascii=False),
                }
            )

        upsert_movie(conn, movie_record)

        if genre_list:
            upsert_movie_genres(conn, movie_id, genre_list)

    save_cache(OMDB_CACHE_FILE, cache)

    load_ratings(conn, ratings_df)

    conn.close()

    print("ETL finished successfully")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--movies", required=True)
    p.add_argument("--ratings", required=True)
    p.add_argument("--apikey", required=False)
    p.add_argument("--db", required=True, help="PostgreSQL connection string")
    p.add_argument("--schema", default="schema.sql")
    args = p.parse_args()
    main(args)
