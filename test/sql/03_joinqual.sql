-- Make sure sure we'll see at least one qual
SET pg_qualstats.sample_rate = 1;

-- test join quals, using simplified version of tpch tables
CREATE TABLE part (
        p_partkey integer,
        p_brand char(10),
        p_container char(10)
);

CREATE TABLE lineitem (
        l_orderkey bigint,
        l_partkey integer,
        l_quantity numeric
);

--------------------
-- Test Hash Join --
--------------------

SET enable_hashjoin = true;
SET enable_mergejoin = false;

-- Make sure that installcheck won't find previous data
SELECT "PGQS".pg_qualstats_reset();

EXPLAIN (COSTS OFF) SELECT count(*)
FROM lineitem, part
WHERE
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
      select
        0.2 * avg(l_quantity)
      FROM
        lineitem
      WHERE
        l_partkey = p_partkey
);

SELECT lc.relname, la.attname,
    opno::regoperator,
    rc.relname, ra.attname,
    constvalue
FROM "PGQS".pg_qualstats pgqs
LEFT JOIN pg_class lc ON lc.oid = pgqs.lrelid
LEFT JOIN pg_attribute la ON la.attrelid = lc.oid AND la.attnum = pgqs.lattnum
LEFT JOIN pg_class rc ON rc.oid = pgqs.rrelid
LEFT JOIN pg_attribute ra ON ra.attrelid = rc.oid AND ra.attnum = pgqs.rattnum
ORDER BY 1, 2, 4, 5, 3;

---------------------
-- Test Merge Join --
---------------------

SET enable_hashjoin = false;
SET enable_mergejoin = true;

-- Make sure that installcheck won't find previous data
SELECT "PGQS".pg_qualstats_reset();

EXPLAIN (COSTS OFF) SELECT count(*)
FROM lineitem, part
WHERE
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
      select
        0.2 * avg(l_quantity)
      FROM
        lineitem
      WHERE
        l_partkey = p_partkey
);

SELECT lc.relname, la.attname,
    opno::regoperator,
    rc.relname, ra.attname,
    constvalue
FROM "PGQS".pg_qualstats pgqs
LEFT JOIN pg_class lc ON lc.oid = pgqs.lrelid
LEFT JOIN pg_attribute la ON la.attrelid = lc.oid AND la.attnum = pgqs.lattnum
LEFT JOIN pg_class rc ON rc.oid = pgqs.rrelid
LEFT JOIN pg_attribute ra ON ra.attrelid = rc.oid AND ra.attnum = pgqs.rattnum
ORDER BY 1, 2, 4, 5, 3;
