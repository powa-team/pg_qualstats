----------------
-- index advisor
----------------

-- Make sure sure we'll see all the quals
SET pg_qualstats.sample_rate = 1;

-- check that empty arrays are returned rather than NULL values
SELECT "PGQS".pg_qualstats_reset();
SELECT * FROM "PGQS".pg_qualstats_index_advisor(50);
-- Test some naive scenario
CREATE TABLE adv (id1 integer, id2 integer, id3 integer, val text);
INSERT INTO adv SELECT i, i, i, 'line ' || i from generate_series(1, 1000) i;
SELECT "PGQS".pg_qualstats_reset();
SELECT * FROM adv WHERE id1 < 0;
SELECT count(*) FROM adv WHERE id1 < 500;
SELECT * FROM adv WHERE val = 'meh';
SELECT * FROM adv WHERE id1 = 0 and val = 'meh';
SELECT * FROM adv WHERE id1 = 1 and val = 'meh';
SELECT * FROM adv WHERE id1 = 1 and id2 = 2 AND val = 'meh';
SELECT * FROM adv WHERE id1 = 6 and id2 = 6 AND id3 = 6 AND val = 'meh';
SELECT COUNT(*) FROM pgqs WHERE id = 1;
-- non optimisable statements
SELECT * FROM adv WHERE val ILIKE 'moh';
SELECT count(*) FROM adv WHERE val ILIKE 'moh';
SELECT * FROM adv WHERE val LIKE 'moh';

-- check the results
SELECT v->'ddl' AS v
  FROM json_array_elements(
    "PGQS".pg_qualstats_index_advisor(50)->'indexes') v
  ORDER BY v::text COLLATE "C";
SELECT v->'qual' AS v
  FROM json_array_elements(
    "PGQS".pg_qualstats_index_advisor(50)->'unoptimised') v
  ORDER BY v::text COLLATE "C";
-- check quals on removed table
DROP TABLE pgqs;
SELECT v->'ddl' AS v
  FROM json_array_elements(
    "PGQS".pg_qualstats_index_advisor(50)->'indexes') v
  ORDER BY v::text COLLATE "C";
