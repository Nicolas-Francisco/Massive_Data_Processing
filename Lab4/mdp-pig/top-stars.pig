-- This script finds the actors/actresses with the highest number of good movies

raw_roles = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars-test-g8.tsv' USING PigStorage('\t') AS (star, title, year, num, type, episode, billing, char, gender);
-- Later you can change the above file to 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars.tsv' to see the full output


raw_ratings = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings-test-g8.tsv' USING PigStorage('\t') AS (dist, votes, score, title, year, num, type, episode);
-- Later you can change the above file to 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings.tsv' to see the full output

--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------
-- we need to join the two tables to get the number of good movies for each actor/actress
-- first, we filter out the non thearical movies
movies = FILTER raw BY type == 'THEATRICAL_MOVIE';

-- Then, we generate new relation for roles and ratings based on the title, year and num of each movie.
-- We do this for the roles and the ratings, respectively.
full_roles = FOREACH raw_roles GENERATE CONCAT(title,'##',year,'##',num) AS movie, star, gender;
full_ratings = FOREACH raw_ratings GENERATE CONCAT(title,'##',year,'##',num) AS movie, votes, score;

movie_stars_ratings = JOIN full_roles BY movie, full_ratings BY movie;

-- We want to compute the top actors / top actresses (separately).
-- Actors should be one output file, actresses in the other.
-- Gender is now given as 'MALE'/'FEMALE' in the gender column of raw_roles
-- To do so, we want to count how many good movies each starred in.
-- We count a movie as good if:
--   it has at least (>=) 10,001 votes (votes in raw_rating) 
--   it has a score >= 7.8 (score in raw_rating)
-- The best actors/actresses are those with the most good movies.
best_movies = FILTER movie_stars_ratings BY votes >= 10001 AND score >= 7.8;

-- we get the count of good movies for each actor/actress
actors_grouped = GROUP best_movies BY (star, gender);
actors_good_movies_count = FOREACH actors_grouped GENERATE COUNT($1) AS count, group AS star;

STORE actors_good_movies_count INTO 'hdfs://cm:9000/uhadoop2022/blackfire/lab4-test-count2/';

-- now we separate the dataset based on the actor/actress gender

-- An actor/actress plays one role in each movie 
--   (more accurately, the roles are concatenated on one line like "role A/role B")

-- If an actor/actress does not star in a good movie
--  a count of zero should be returned (i.e., the actor/actress
--   should still appear in the output),
-- The results should be sorted descending by count.

-- Test on smaller file first (as given above),
--  then test on larger file to get the results.

--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------
