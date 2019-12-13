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
