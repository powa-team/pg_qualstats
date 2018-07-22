CREATE EXTENSION pg_qualstats;

-- Make sure sure we'll see at least one qual
SET pg_qualstats.sample_rate = 1;

CREATE TABLE pgqs AS SELECT id FROM generate_series(1, 100) id;
SELECT COUNT(*) FROM pgqs WHERE id = 1;
SELECT lrelid::regclass::text, lattnum, occurences, execution_count,
    nbfiltered, constvalue, eval_type
FROM pg_qualstats;
SELECT COUNT(*) > 0 FROM pg_qualstats;
SELECT COUNT(*) > 0 FROM pg_qualstats();
SELECT COUNT(*) > 0 FROM pg_qualstats_example_queries();
SELECT pg_qualstats_reset();
SELECT COUNT(*) FROM pg_qualstats();
