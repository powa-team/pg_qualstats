-- Make sure that installcheck won't find previous data
SELECT "PGQS".pg_qualstats_reset();

-- Make sure sure we'll see at least one qual
SET pg_qualstats.sample_rate = 1;

CREATE TABLE pgqs AS SELECT id, 'a'::text val FROM generate_series(1, 100) id;
SELECT COUNT(*) FROM pgqs WHERE id = 1;
SELECT lrelid::regclass::text, lattnum, occurences, execution_count,
    nbfiltered, constvalue, eval_type
FROM "PGQS".pg_qualstats;
SELECT COUNT(*) > 0 FROM "PGQS".pg_qualstats;
SELECT COUNT(*) > 0 FROM "PGQS".pg_qualstats();
SELECT COUNT(*) > 0 FROM "PGQS".pg_qualstats_example_queries();
SELECT "PGQS".pg_qualstats_reset();
SELECT COUNT(*) FROM "PGQS".pg_qualstats();

-- OpExpr sanity checks
-- subquery_var operator const, shouldn't be tracked
SELECT * FROM (SELECT * FROM pgqs LIMIT 0) pgqs WHERE pgqs.id = 0;
SELECT COUNT(*) FROM "PGQS".pg_qualstats();

-- const non_commutable_operator var, should be tracked, var found on RHS
SELECT * FROM pgqs WHERE 'meh' ~ val;
SELECT lrelid::regclass, lattnum, rrelid::regclass, rattnum FROM "PGQS".pg_qualstats();
SELECT "PGQS".pg_qualstats_reset();

-- opexpr operator var and commuted, shouldn't be tracked
SELECT * FROM pgqs WHERE id % 2 = 3;
SELECT * FROM pgqs WHERE 3 = id % 2;
SELECT COUNT(*) FROM "PGQS".pg_qualstats();

-- same query with handled commuted qual, which should be found as identical
SELECT * FROM pgqs WHERE id = 0;
SELECT * FROM pgqs WHERE 0 = id;
SELECT lrelid::regclass, lattnum, rrelid::regclass, rattnum, sum(occurences)
FROM "PGQS".pg_qualstats()
GROUP by 1, 2, 3, 4;
SELECT COUNT(DISTINCT qualnodeid) FROM "PGQS".pg_qualstats();

-- (unique)qualid behavior
SELECT "PGQS".pg_qualstats_reset();

-- There should be one group of 2 AND-ed quals, and 1 qual alone
SELECT COUNT(*) FROM pgqs WHERE (id = 1) OR (id > 10 AND id < 20);
SELECT CASE WHEN qualid IS NULL THEN 'OR-ed' ELSE 'AND-ed' END kind, COUNT(*)
FROM "PGQS".pg_qualstats() GROUP BY 1 ORDER BY 2 DESC;
