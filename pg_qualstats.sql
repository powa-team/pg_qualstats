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
  OUT nodehash 	  int,
  OUT count bigint
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
) t GROUP BY relid, attnames, possible_types
