CREATE EXTENSION pg_qualstats;

-- Make sure sure we'll see at least one qual
SET pg_qualstats.sample_rate = 1;

CREATE TABLE pgqs AS SELECT id, 'a' val FROM generate_series(1, 100) id;
SELECT COUNT(*) FROM pgqs WHERE id = 1;
SELECT lrelid::regclass::text, lattnum, occurences, execution_count,
    nbfiltered, constvalue, eval_type
FROM pg_qualstats;
SELECT COUNT(*) > 0 FROM pg_qualstats;
SELECT COUNT(*) > 0 FROM pg_qualstats();
SELECT COUNT(*) > 0 FROM pg_qualstats_example_queries();
SELECT pg_qualstats_reset();
SELECT COUNT(*) FROM pg_qualstats();
-- OpExpr sanity checks
-- subquery_var operator const, shouldn't be tracked
SELECT * FROM (SELECT * FROM pgqs LIMIT 0) pgqs WHERE pgqs.id = 0;
SELECT COUNT(*) FROM pg_qualstats();
-- const non_commutable_operator var, should be tracked, var found on RHS
SELECT * FROM pgqs WHERE 'somevalue' ^@ val;
SELECT lrelid::regclass, lattnum, rrelid::regclass, rattnum FROM pg_qualstats();
SELECT pg_qualstats_reset();
-- opexpr operator var and commuted, shouldn't be tracked
SELECT * FROM pgqs WHERE id % 2 = 3;
SELECT * FROM pgqs WHERE 3 = id % 2;
SELECT COUNT(*) FROM pg_qualstats();
-- same query with handled commuted qual, which should be found as identical
SELECT * FROM pgqs WHERE id = 0;
SELECT * FROM pgqs WHERE 0 = id;
SELECT lrelid::regclass, lattnum, rrelid::regclass, rattnum FROM pg_qualstats();
SELECT COUNT(DISTINCT qualnodeid) FROM pg_qualstats();
-- (unique)qualid behavior
SELECT pg_qualstats_reset();
-- There should be one group of 2 AND-ed quals, and 1 qual alone
SELECT COUNT(*) FROM pgqs WHERE (id = 1) OR (id > 10 AND id < 20);
SELECT CASE WHEN qualid IS NULL THEN 'OR-ed' ELSE 'AND-ed' END kind, COUNT(*) FROM pg_qualstats() GROUP BY 1 ORDER BY 2 DESC;
-- index advisor
CREATE TABLE adv (id1 integer, id2 integer, id3 integer, val text);
INSERT INTO adv SELECT i, i, i, 'line ' || i from generate_series(1, 1000) i;
SELECT pg_qualstats_reset();
SELECT * FROM adv WHERE id1 < 0;
SELECT count(*) FROM adv WHERE id1 < 500;
SELECT * FROM adv WHERE val = 'meh';
SELECT * FROM adv WHERE id1 = 0 and val = 'meh';
SELECT * FROM adv WHERE id1 = 1 and val = 'meh';
SELECT * FROM adv WHERE id1 = 1 and id2 = 2 AND val = 'meh';
SELECT * FROM adv WHERE id1 = 6 and id2 = 6 AND id3 = 6 AND val = 'meh';
SELECT * FROM adv WHERE val ILIKE 'moh';
SELECT COUNT(*) FROM pgqs WHERE id = 1;
SELECT v
  FROM jsonb_array_elements(
    pg_qualstats_index_advisor(min_filter => 50)->'indexes') v
  ORDER BY v::text COLLATE "C";
SELECT v
  FROM jsonb_array_elements(
    pg_qualstats_index_advisor(min_filter => 50)->'unoptimised') v
  ORDER BY v::text COLLATE "C";
-- check quals on removed table
DROP TABLE pgqs;
SELECT v
  FROM jsonb_array_elements(
    pg_qualstats_index_advisor(min_filter => 50)->'indexes') v
  ORDER BY v::text COLLATE "C";
