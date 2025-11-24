# movie-datapipeline
Movie Data Pipeline – Data Engineering Assignment

Overview

This project implements a complete Movie Data ETL pipeline that:
	•	Reads movies.csv and ratings.csv from the MovieLens dataset
	•	Enriches movie data using the OMDb API (IMDb metadata)
	•	Cleans and transforms raw data (genres, years, timestamps)
	•	Loads the final structured dataset into a PostgreSQL database
	•	Executes analytical queries to answer business questions

Designed with idempotency, clean relational modeling, and API‑driven enrichment in mind.


Tech Stack
	•	Python 3
	•	PostgreSQL
	•	pandas, SQLAlchemy, PyPostgreSQL
	•	requests
	•	python-dotenv


Setup Instructions

1. Create virtual environment
  python -m venv venv
  venv\Scripts\activate
  pip install -r requirements.txt

2. Create PostgreSQL database
   CREATE DATABASE moviesdb;

   Run schema.sql

   
3. Configure environment variables

Create a .env file:
DB_URL=postgresql+psycopg2://postgres:Admin@localhost:5432/moviesdb
OMDB_API_KEY='MY API KEY'

4. Prepare input dataset

Download MovieLens ml-latest-small dataset.
Place these files in the project root:
movies.csv
ratings.csv

5. Run the ETL pipeline
    python etl.py --movies movies.csv --ratings ratings.csv

This will:
	•	Parse titles & release years
	•	Fetch OMDb fields (imdb_id, director, box_office, runtime, language, country)
	•	Normalize genres into genres + movie_genres
	•	Convert UNIX timestamps to DATETIME
	•	Perform full refresh (truncate + reload)
	•	movies
	•	genres
	•	movie_genres
	•	users
	•	ratings

The pipeline is idempotent — every run starts clean.


Repository Structure
etl.py
schema.sql
queries.sql
README.md
requirements.txt
omdb_cache.json
movies.csv
ratings.csv


   Data Model

movies
	•	movieId (PK)
	•	title
	•	release_year
	•	imdb_id
	•	director
	•	box_office
	•	runtime_minutes
	•	language
	•	country

genres
	•	genre_id (PK)
	•	name (unique)

movie_genres
	•	movieId (FK → movies.movieId)
	•	genre_id (FK → genres.genre_id)
	•	Composite PK (movieId, genre_id)

users
	•	userId (PK)

ratings
	•	userId (FK → users.userId)
	•	movieId (FK → movies.movieId)
	•	rating
	•	rating_timestamp
	•	Composite PK (userId, movieId, rating_timestamp)

Key design decisions:
	•	Genres normalized to support many‑to‑many relationships.
	•	Users stored as a dedicated table for a more realistic OLTP structure.
	•	Full‑refresh ETL simplifies idempotency and keeps data consistent.

⸻

Running Analytical Queries

Open queries.sql in PostgreSQL Workbench.
Queries answer:
	•	Highest‑rated movie
	•	Top 5 genres by average rating
	•	Director with the most movies (via OMDb metadata)
	•	Year‑wise average rating trends

⸻

Error Handling & Data Quality
	•	Missing OMDb data: movie loads with NULL enrichment fields.
	•	Parsing issues: invalid runtime/box office/year → NULL.
	•	Title parsing: extract (year) when possible.
	•	Rate limiting: small time.sleep() between OMDb calls.

⸻

Challenges & Solutions

1. Special characters in DB password

Using characters like @ breaks URL connection strings.
Solution: Use sqlalchemy.engine.URL.create().

2. Foreign key issues in sample tests

Ratings referenced movies not included in subsets.
Solution: Filter ratings in test mode.

3. OMDb title mismatches

Some MovieLens titles do not match OMDb.
Solution: Store NULLs, never break the pipeline.

4. Rate limiting

OMDb throttles large sequential requests.
Solution: Add short delays.

⸻

Requirements
pandas
SQLAlchemy
PyPostgreSQL
requests
python-dotenv
openpyxl



This ETL pipeline is clean, reproducible, and designed with proper separation of concerns:
	•	Extraction (MovieLens + OMDb)
	•	Transformation (pandas normalization)
	•	Loading (PostgreSQL relational schema)
