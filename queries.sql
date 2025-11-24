-- queries.sql

-- 1) Which movie has the highest average rating?
-- (If multiple movies tie, this returns them all.)
SELECT m.movie_id, m.title, ROUND(AVG(r.rating), 3) AS avg_rating, COUNT(r.rating) AS n_ratings
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id, m.title
HAVING COUNT(r.rating) >= 1
ORDER BY avg_rating DESC, n_ratings DESC
LIMIT 1;


-- 2) Top 5 movie genres that have the highest average rating
-- We compute average rating per genre by joining movie_genres -> movies -> ratings
SELECT g.name AS genre, ROUND(AVG(r.rating), 3) AS avg_rating, COUNT(DISTINCT m.movie_id) AS n_movies
FROM genres g
JOIN movie_genres mg ON g.id = mg.genre_id
JOIN movies m ON mg.movie_id = m.movie_id
JOIN ratings r ON r.movie_id = m.movie_id
GROUP BY g.id, g.name
HAVING COUNT(r.rating) >= 5 -- optional filter to avoid tiny-sample noise
ORDER BY avg_rating DESC
LIMIT 5;


-- 3) Who is the director with the most movies in this dataset?
SELECT director, COUNT(*) AS movie_count
FROM movies
WHERE director IS NOT NULL AND TRIM(director) != ''
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;


-- 4) What is the average rating of movies released each year?
SELECT m.year, ROUND(AVG(r.rating), 3) AS avg_rating, COUNT(r.rating) AS n_ratings
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
WHERE m.year IS NOT NULL
GROUP BY m.year
ORDER BY m.year;