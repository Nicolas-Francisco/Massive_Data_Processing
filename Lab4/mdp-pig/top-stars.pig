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

-- First, we split the data into two groups: good movies and bad movies
SPLIT movie_stars_ratings INTO good_movies IF votes >= 10001 AND score >= 7.8, bad_movies IF votes < 10001 OR score < 7.8;

-- now we get the count of good movies and bad movies for each actor/actress
-- we do this by counting the number of movies each actor/actress starred in
good_movies_count = GROUP good_movies BY (star, gender);
actors_good_movies_count = FOREACH good_movies_count GENERATE COUNT($1) AS count, FLATTEN(group) as (star, gender);

bad_movies_count = GROUP bad_movies BY (star, gender);
actors_bad_movies_count = FOREACH bad_movies_count GENERATE 0 AS count, FLATTEN(group) as (star, gender);

-- now we get the union of the actors counts, and the split them based on their genders
all_actors_count_raw = UNION actors_good_movies_count, actors_bad_movies_count;

-- now we separate the dataset based on the actor/actress gender
SPLIT all_actors_count_raw INTO male_actors IF gender == 'MALE', female_actresses IF gender == 'FEMALE';

STORE male_actors INTO 'hdfs://cm:9000/uhadoop2022/blackfire/lab4-test-male/';
STORE female_actresses INTO 'hdfs://cm:9000/uhadoop2022/blackfire/lab4-test-female/';


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
