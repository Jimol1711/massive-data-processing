--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------
-- This Pig script finds the actors with the highest number of good and bad movies

-- We want to compute the best actors and worst actors (independently of gender).
-- Best actors should be one output file, worst actors in the other.

-- For best actors, we want to count how many good movies each starred in.
-- We count a movie as good if:
--   it has at least (>=) 1,000 votes (votes in raw_rating) 
--   it has a score >= 8.0 (score in raw_rating)

-- The best actors are those with the most good movies.

-- For worst actors, we want to count how many bad movies each starred in.
-- We count a movie as bad if:
--   it has at least (>=) 1,000 votes (votes in raw_rating) 
--   it has a score <= 3.0 (score in raw_rating)

-- The worst actors are those with the most bad movies.

-- An actor plays one role in each movie 
--   (more accurately, the roles are concatenated on one line like "role A/role B")

-- If an actor does not star in a good/bad movie
--  a count of zero should be returned (i.e., the actor
--   should still appear in the best/worst output, respectively).

-- The results should be sorted descending by count.

-- We only want to count entries of type movie (not tv series, etc.).
-- Again, note that only CONCAT(title,'##',(chararray)year) acts as a key for movies.

-- Test on smaller file first (as given above),
--  then test on full file to get the results.

--------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------

-- Load data with actor-movie relation
raw_roles = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars.tsv' USING PigStorage('\t') AS (star:chararray, title:chararray, year:int, type:chararray, char:chararray, gender:chararray);
-- Later you can change the above file to 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars.tsv' to see the full output

-- Load data with movie ratings
raw_ratings = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings.tsv' USING PigStorage('\t') AS (votes:int, score:double, title:chararray, year:int, type:chararray, episode:chararray);
-- Later you can change the above file to 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings.tsv' to see the full output

-- DUMP raw_roles;
-- DUMP raw_ratings;

-- (13343,9.2,Covfefe I,2014,movie,null)

-- We filter only movies
roles_filtered = FILTER raw_roles BY type == 'movie';
ratings_filtered = FILTER raw_ratings BY type == 'movie';

-- We filter good and bad movies
good_movies = FILTER ratings_filtered BY votes >= 1000 AND score >= 8.0;
bad_movies = FILTER ratings_filtered BY votes >= 1000 AND score <= 3.0;

-- DUMP good_movies;
-- DUMP bad_movies;

-- TODO: FINISH THE SCRIPT
-- Create join key
roles_with_key = FOREACH roles_filtered GENERATE star, CONCAT(title, '##', (chararray)year) AS movie_key;
good_with_key = FOREACH good_movies GENERATE CONCAT(title, '##', (chararray)year) AS movie_key;
bad_with_key = FOREACH bad_movies GENERATE CONCAT(title, '##', (chararray)year) AS movie_key;

-- Join actors with good/bad movies
good_roles = JOIN roles_with_key BY movie_key LEFT OUTER, good_with_key BY movie_key;
bad_roles = JOIN roles_with_key BY movie_key LEFT OUTER, bad_with_key BY movie_key;

-- Map presence of match to flags
good_flags = FOREACH good_roles GENERATE roles_with_key::star AS star, 
                                  (good_with_key::movie_key IS NOT NULL ? 1 : 0) AS is_good;

bad_flags = FOREACH bad_roles GENERATE roles_with_key::star AS star, 
                                (bad_with_key::movie_key IS NOT NULL ? 1 : 0) AS is_bad;

-- Group and count
good_grouped = GROUP good_flags BY star;
bad_grouped = GROUP bad_flags BY star;

good_actors = FOREACH good_grouped GENERATE group AS star, SUM(good_flags.is_good) AS good_count;
bad_actors = FOREACH bad_grouped GENERATE group AS star, SUM(bad_flags.is_bad) AS bad_count;

-- Sort descending
good_actors_ordered = ORDER good_actors BY good_count DESC;
bad_actors_ordered = ORDER bad_actors BY bad_count DESC;

-- DUMP first ten for each list of best and worst actors
first_ten_best = LIMIT good_actors_ordered 10;
DUMP first_ten_best;

first_ten_worst = LIMIT bad_actors_ordered 10;
DUMP first_ten_worst;

STORE good_actors_ordered INTO 'hdfs://cm:9000/uhadoop2025/holaquetal/imdb-goodactors-full/';
STORE bad_actors_ordered INTO 'hdfs://cm:9000/uhadoop2025/holaquetal/imdb-badactors-full/';

STORE first_ten_best INTO 'hdfs://cm:9000/uhadoop2025/holaquetal/imdb-goodactors-10/';
STORE first_ten_worst INTO 'hdfs://cm:9000/uhadoop2025/holaquetal/imdb-badactors-10/';

-- good and bad harrison ford counts
harrison_good = FILTER good_actors BY star == 'Harrison Ford';
harrison_bad = FILTER bad_actors BY star == 'Harrison Ford';

STORE harrison_good INTO 'hdfs://cm:9000/uhadoop2025/holaquetal/harrison-good/';
STORE harrison_bad INTO 'hdfs://cm:9000/uhadoop2025/holaquetal/harrison-bad/';

-- Best 10:
-- Kamal Haasan    29
-- Mohanlal        28
-- Nassar  25
-- Prakash Raj     18
-- Mammootty       18
-- Kemal Sunal     17
-- Brahmanandam    17
-- Sener Sen       17
-- Adile Nasit     16
-- Thilakan        16

-- Worst 10:
-- Michael Madsen  10
-- Eric Roberts    10
-- Mehmet Ali Erbil        7
-- Nicole D'Angelo 7
-- Michael Paré    7
-- Danny Trejo     6
-- Jane Alexander  6
-- Akshay Kumar    6
-- Alexander Nevsky        6
-- Chris Spinelli  6

-- Harrison Ford good and bad counts respectively:
-- Harrison Ford   8
-- Harrison Ford   0