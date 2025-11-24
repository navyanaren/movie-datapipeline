DROP TABLE IF EXISTS ratings CASCADE;
DROP TABLE IF EXISTS movie_genres CASCADE;
DROP TABLE IF EXISTS genres CASCADE;
DROP TABLE IF EXISTS movies CASCADE;

CREATE TABLE movies(
  movie_id INTEGER PRIMARY KEY,
  title TEXT NOT NULL,
  year INTEGER,
  director TEXT,
  plot TEXT,
  box_office TEXT,
  imdb_id TEXT,
  omdb_raw TEXT
);

CREATE TABLE genres(
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE movie_genres(
  movie_id INTEGER NOT NULL,
  genre_id INTEGER NOT NULL,
  PRIMARY KEY (movie_id, genre_id),
  FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
  FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
);

CREATE TABLE ratings(
  user_id INTEGER NOT NULL,
  movie_id INTEGER NOT NULL,
  rating NUMERIC NOT NULL,
  timestamp BIGINT,
  PRIMARY KEY (user_id, movie_id, timestamp),
  FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE
);

CREATE INDEX idx_ratings_movie ON ratings(movie_id);
CREATE INDEX idx_movies_year ON movies(year);
