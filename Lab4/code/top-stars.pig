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
raw_roles = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars-test.tsv' USING PigStorage('\t') AS (star:chararray, title:chararray, year:int, type:chararray, char:chararray, gender:chararray);
-- Later you can change the above file to 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars.tsv' to see the full output

-- Load data with movie ratings
raw_ratings = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings-test.tsv' USING PigStorage('\t') AS (votes:int, score:double, title:chararray, year:int, type:chararray, episode:chararray);
-- Later you can change the above file to 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings.tsv' to see the full output


-- Load data with actor-movie relation
raw_roles = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars-test.tsv' 
    USING PigStorage('\t') AS (star:chararray, title:chararray, year:int, type:chararray, char:chararray, gender:chararray);

-- Load data with movie ratings
raw_ratings = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings-test.tsv' 
    USING PigStorage('\t') AS (votes:int, score:double, title:chararray, year:int, type:chararray, episode:chararray);

DUMP raw_roles;
DUMP raw_ratings;

-- (13343,9.2,Covfefe I,2014,movie,null)
