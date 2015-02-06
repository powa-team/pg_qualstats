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
  OUT qualid  bigint,
  OUT uniquequalid bigint,
  OUT qualnodeid    bigint,
  OUT uniquequalnodeid bigint,
  OUT count bigint,
  OUT nbfiltered bigint,
  OUT constant_position int,
  OUT queryid    bigint,
  OUT constvalue varchar,
  OUT eval_type  "char"
)
RETURNS SETOF record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION pg_qualstats_names(
  OUT userid oid,
  OUT dbid oid,
  OUT lrelid oid,
  OUT lattnum smallint,
  OUT opno oid,
  OUT rrelid oid,
  OUT rattnum smallint,
  OUT qualid  bigint,
  OUT uniquequalid bigint,
  OUT qualnodeid    bigint,
  OUT uniquequalnodeid bigint,
  OUT count bigint,
  OUT nbfiltered bigint,
  OUT constant_position int,
  OUT queryid    bigint,
  OUT constvalue varchar,
  OUT eval_type  "char",
  OUT rolname text,
  OUT dbname text,
  OUT lrelname text,
  OUT lattname  text,
  OUT opname text,
  OUT rrelname text,
  OUT rattname text
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
        sum(count) as count,
        sum(nbfiltered) as nbfiltered
  from pg_qualstats qs
  left join (pg_class cl inner join pg_namespace nl on nl.oid = cl.relnamespace) on cl.oid = qs.lrelid
  left join (pg_class cr inner join pg_namespace nr on nr.oid = cr.relnamespace) on cr.oid = qs.rrelid
  left join pg_attribute al on al.attrelid = qs.lrelid and al.attnum = qs.lattnum
  left join pg_attribute ar on ar.attrelid = qs.rrelid and ar.attnum = qs.rattnum
  group by al.attrelid, al.attname, ar.attrelid, ar.attname, opno, nl.nspname, nr.nspname
;



CREATE OR REPLACE VIEW pg_qualstats_all AS
  SELECT dbid, relid, userid, queryid, array_agg(distinct attnum) as attnums, opno, max(qualid) as qualid, sum(count) as count,
    coalesce(qualid, qualnodeid) as qualnodeid
  FROM (
    SELECT
          qs.dbid,
          CASE WHEN lrelid IS NOT NULL THEN lrelid
               WHEN rrelid IS NOT NULL THEN rrelid
          END as relid,
          qs.userid as userid,
          CASE WHEN lrelid IS NOT NULL THEN lattnum
               WHEN rrelid IS NOT NULL THEN rattnum
          END as attnum,
          qs.opno as opno,
          qs.qualid as qualid,
          qs.qualnodeid as qualnodeid,
          qs.count as count,
          qs.queryid
    FROM pg_qualstats() qs
    WHERE lrelid IS NOT NULL or rrelid IS NOT NULL
  ) t GROUP BY dbid, relid, userid, queryid, opno, coalesce(qualid, qualnodeid)
;

CREATE TYPE qual AS (
  relid oid,
  attnum integer,
  opno oid,
  eval_type "char"
 );

CREATE TYPE qualname AS (
  relname text,
  attnname text,
  opname text,
  eval_type "char"
);

CREATE OR REPLACE VIEW pg_qualstats_by_query AS
        SELECT coalesce(uniquequalid, uniquequalnodeid) as uniquequalnodeid, dbid, userid,  coalesce(qualid, qualnodeid) as qualnodeid, count, nbfiltered, queryid,
      array_agg(distinct constvalue) as constvalues, array_agg(distinct ROW(relid, attnum, opno, eval_type)::qual) as quals
      FROM
      (

        SELECT
            qs.dbid,
            CASE WHEN lrelid IS NOT NULL THEN lrelid
                WHEN rrelid IS NOT NULL THEN rrelid
            END as relid,
            qs.userid as userid,
            CASE WHEN lrelid IS NOT NULL THEN lattnum
                WHEN rrelid IS NOT NULL THEN rattnum
            END as attnum,
            qs.opno as opno,
            qs.qualid as qualid,
            qs.uniquequalid as uniquequalid,
            qs.qualnodeid as qualnodeid,
            qs.uniquequalnodeid as uniquequalnodeid,
            qs.count as count,
            qs.queryid as queryid,
            qs.constvalue as constvalue,
            qs.nbfiltered as nbfiltered,
            qs.eval_type
        FROM pg_qualstats() qs
        WHERE (qs.lrelid IS NULL) != (qs.rrelid IS NULL)
    ) i GROUP BY coalesce(uniquequalid, uniquequalnodeid), coalesce(qualid, qualnodeid),  dbid, userid, count, nbfiltered, queryid
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
  GROUP BY qs.relid, qualnodeid
) t GROUP BY relid, attnames, possible_types;



-- Fonction pour analyse "après coup", à partir de données historisées par
-- exemple
CREATE OR REPLACE FUNCTION pg_qualstats_suggest_indexes(relid oid, attnums integer[], opno oid) RETURNS TABLE(index_ddl text) AS $$
BEGIN
RETURN QUERY
 SELECT 'CREATE INDEX idx_' || q.relid || '_' || array_to_string(attnames, '_') || ' ON ' || nspname || '.' || q.relid ||  ' USING ' || idxtype || ' (' || array_to_string(attnames, ', ') || ')'  AS index_ddl
 FROM (SELECT t.nspname,
    t.relid,
    t.attnames,
    unnest(t.possible_types) AS idxtype

   FROM ( SELECT nl.nspname AS nspname,
            qs.relid::regclass AS relid,
            array_agg(DISTINCT attnames.attnames) AS attnames,
            array_agg(DISTINCT pg_am.amname) AS possible_types,
            array_agg(DISTINCT attnum.attnum) AS attnums
           FROM (VALUES (relid, attnums::smallint[], opno)) as qs(relid, attnums, opno)
           LEFT JOIN (pg_class cl JOIN pg_namespace nl ON nl.oid = cl.relnamespace) ON cl.oid = qs.relid
           JOIN pg_amop amop ON amop.amopopr = qs.opno
           JOIN pg_am ON amop.amopmethod = pg_am.oid AND pg_am.amname <> 'hash',
           LATERAL ( SELECT pg_attribute.attname AS attnames
                       FROM pg_attribute
                       JOIN unnest(qs.attnums) a(a) ON a.a = pg_attribute.attnum AND pg_attribute.attrelid = qs.relid
                      ORDER BY pg_attribute.attnum) attnames,
           LATERAL unnest(qs.attnums) attnum(attnum)
          WHERE NOT (EXISTS ( SELECT 1
                               FROM pg_index i
                              WHERE i.indrelid = qs.relid AND ((i.indkey::smallint[])[0:array_length(qs.attnums, 1) - 1] @> qs.attnums OR qs.attnums @> (i.indkey::smallint[])[0:array_length(i.indkey, 1) + 1] AND i.indisunique)))
          GROUP BY nl.nspname, qs.relid) t
  GROUP BY t.nspname, t.relid, t.attnames, t.possible_types) q;
END;
$$ language plpgsql;

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
          GROUP BY nl.nspname, qs.relid, qs.qualnodeid) t
  GROUP BY t.nspname, t.relid, t.attnames, t.possible_types) q;
