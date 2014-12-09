CREATE FUNCTION pg_qualstats_reset()
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION pg_qualstats(
  OUT userid oid,
  OUT dbid oid,
  OUT lrelid oid,
  OUT lattnum smallint,
  OUT opno oid,
  OUT rrelid oid,
  OUT rattnum smallint,
  OUT parenthash  int,
  OUT nodehash    int,
  OUT count bigint,
  OUT queryid int,
  OUT constvalue varchar
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

-- Register a view on the function for ease of use.
CREATE VIEW pg_qualstats AS
  SELECT qs.* FROM pg_qualstats() qs
  INNER JOIN pg_database on qs.dbid = pg_database.oid
  WHERE pg_database.datname = current_database();


GRANT SELECT ON pg_qualstats TO PUBLIC;

-- Don't want this to be available to non-superusers.
REVOKE ALL ON FUNCTION pg_qualstats_reset() FROM PUBLIC;


CREATE VIEW pg_qualstats_pretty AS
  select
        nl.nspname as left_schema,
        al.attrelid::regclass as left_table,
        al.attname as left_column,
        opno::regoper as operator,
        nr.nspname as right_schema,
        ar.attrelid::regclass as right_table,
        ar.attname as right_column,
        sum(count) as count
  from pg_qualstats qs
  left join (pg_class cl inner join pg_namespace nl on nl.oid = cl.relnamespace) on cl.oid = qs.lrelid
  left join (pg_class cr inner join pg_namespace nr on nr.oid = cr.relnamespace) on cr.oid = qs.rrelid
  left join pg_attribute al on al.attrelid = qs.lrelid and al.attnum = qs.lattnum
  left join pg_attribute ar on ar.attrelid = qs.rrelid and ar.attnum = qs.rattnum
  group by al.attrelid, al.attname, ar.attrelid, ar.attname, opno, nl.nspname, nr.nspname
  ;



CREATE VIEW pg_qualstats_all AS
  SELECT relid, array_agg(distinct attnum) as attnums, opno, max(parenthash) as parenthash, sum(count) as count,
      coalesce(parenthash, nodehash) as nodehash
  FROM (
    SELECT
          qs.lrelid as relid,
          qs.lattnum as attnum,
          qs.opno as opno,
          qs.parenthash as parenthash,
          qs.nodehash as nodehash,
          qs.count as count
    FROM pg_qualstats qs
    WHERE qs.lrelid IS NOT NULL
    UNION ALL
    SELECT qs.rrelid as relid,
          qs.rattnum as attnum,
          qs.opno as opno,
          qs.parenthash as parenthash,
          qs.nodehash as nodehash,
          count as count
    FROM pg_qualstats qs
    WHERE qs.rrelid IS NOT NULL
  ) t GROUP BY relid, opno, coalesce(parenthash, nodehash)
;

CREATE VIEW pg_qualstats_indexes AS
SELECT relid::regclass, attnames, possible_types, sum(count) as count
FROM (
  SELECT qs.relid::regclass, array_agg(distinct attnames) as attnames, array_agg(distinct amname) as possible_types, max(count) as count, array_agg(distinct attnum) as attnums
  FROM pg_qualstats_all as qs
  INNER JOIN pg_amop amop ON amop.amopopr = opno
  INNER JOIN pg_am on amop.amopmethod = pg_am.oid,
  LATERAL (SELECT attname as attnames from pg_attribute inner join unnest(attnums) a on a = attnum and attrelid = qs.relid order by attnum) as attnames,
  LATERAL unnest(attnums) as attnum
  WHERE NOT EXISTS (
    SELECT 1 from pg_index i
    WHERE indrelid = relid AND (
        (i.indkey::int2[])[0:array_length(attnums, 1) - 1] @> (attnums::int2[]) OR
        ((attnums::int2[]) @> (i.indkey::int2[])[0:array_length(indkey, 1) + 1]  AND
         i.indisunique))
  )
  GROUP BY qs.relid, nodehash
) t GROUP BY relid, attnames, possible_types;

CREATE OR REPLACE VIEW pg_qualstats_indexes_ddl AS
 SELECT q.nspname,
    q.relid,
    q.attnames,
    q.idxtype,
    q.count,
    'CREATE INDEX idx_' || relid || '_' || array_to_string(attnames, '_') || ' ON ' || nspname || '.' || relid ||  ' USING ' || idxtype || ' (' || array_to_string(attnames, ', ') || ')'  AS ddl
 FROM (SELECT t.nspname,
    t.relid,
    t.attnames,
    unnest(t.possible_types) AS idxtype,
    sum(t.count) AS count

   FROM ( SELECT nl.nspname AS nspname,
            qs.relid::regclass AS relid,
            array_agg(DISTINCT attnames.attnames) AS attnames,
            array_agg(DISTINCT pg_am.amname) AS possible_types,
            max(qs.count) AS count,
            array_agg(DISTINCT attnum.attnum) AS attnums
           FROM pg_qualstats_all qs
           LEFT JOIN (pg_class cl JOIN pg_namespace nl ON nl.oid = cl.relnamespace) ON cl.oid = qs.relid
           JOIN pg_amop amop ON amop.amopopr = qs.opno
           JOIN pg_am ON amop.amopmethod = pg_am.oid,
           LATERAL ( SELECT pg_attribute.attname AS attnames
                       FROM pg_attribute
                       JOIN unnest(qs.attnums) a(a) ON a.a = pg_attribute.attnum AND pg_attribute.attrelid = qs.relid
                      ORDER BY pg_attribute.attnum) attnames,
           LATERAL unnest(qs.attnums) attnum(attnum)
          WHERE NOT (EXISTS ( SELECT 1
                               FROM pg_index i
                              WHERE i.indrelid = qs.relid AND ((i.indkey::smallint[])[0:array_length(qs.attnums, 1) - 1] @> qs.attnums OR qs.attnums @> (i.indkey::smallint[])[0:array_length(i.indkey, 1) + 1] AND i.indisunique)))
          GROUP BY nl.nspname, qs.relid, qs.nodehash) t
  GROUP BY t.nspname, t.relid, t.attnames, t.possible_types) q;
