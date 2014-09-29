CREATE TYPE pg_qualstats_history AS (
  ts timestamp with time zone,
  relid oid,
  attnums int[],
  opno oid,
  count bigint
);


CREATE TABLE powaqualstats_history_by_query (
  dbname name,
  queryid bigint,
  coalesce_range tstzrange,
  records pg_qualstats_history[]
);

CREATE TABLE powaqualstats_history_by_query_current (
  dbname name,
  queryid bigint,
  pg_qualstat_history_record pg_qualstats_history
);


-- TODO: merge with powa_statements
CREATE TABLE powaqualstats_statements (
  queryid bigint,
  md5query text,
  rolname text not null,
  dbname text not null,
  query text not null,
  relid oid,
  attnums int[],
  opno oid
);

CREATE OR REPLACE FUNCTION powaqualstats_take_statements_snaphot() RETURNS void as $PROC$
DECLARE
  result bool;
BEGIN
  RAISE DEBUG 'running powaqualstats_take_statements_snaphot';
  WITH capture AS (
    SELECT * FROM pg_qualstats()
  ),
  missing_statements AS (
      INSERT INTO powaqualstats_statements (queryid, md5query, rolname, dbname, query)
        SELECT DISTINCT c.queryid, md5(rolname||datname||query), rolname, datname, ss.query
        FROM capture c INNER JOIN pg_stat_statements ss on ss.queryid = c.queryid
		INNER JOIN pg_roles on pg_roles.oid = c.userid
		INNER JOIN pg_database on pg_database.oid = c.dbid
        WHERE NOT EXISTS (SELECT 1
                          FROM powaqualstats_statements
                          WHERE powaqualstats_statements.queryid = c.queryid)
  ),
  by_query AS (
    INSERT INTO powaqualstats_history_by_query_current (dbname, queryid, pg_qualstat_history_record)
      SELECT datname, queryid, row(now(), relid, attnums,  opno, count::int)::pg_qualstats_history
      FROM pg_qualstats_by_query inner join pg_database on pg_database.oid = dbid
  )
  SELECT true into result;
END
$PROC$ language plpgsql;

CREATE OR REPLACE FUNCTION powaqualstats_statements_aggregate() RETURNS void AS $PROC$
BEGIN
  RAISE DEBUG 'running powaqualstats_statements_aggregate';
  LOCK TABLE powaqualstats_history_by_query_current IN SHARE MODE;
  INSERT INTO powaqualstats_history_by_query
    SELECT dbname, queryid, tstzrange(min((pg_qualstat_history_record).ts), max((pg_qualstat_history_record).ts), '[]'),
      array_agg(pg_qualstat_history_record)
    FROM powaqualstats_history_by_query_current
    GROUP BY dbname, queryid;
  TRUNCATE powaqualstats_history_by_query_current;
END
$PROC$ language plpgsql;


CREATE OR REPLACE FUNCTION powaqualstats_purge() RETURNS void as $PROC$
BEGIN
  RAISE DEBUG 'running powaqualstats_purge';
  DELETE FROM powaqualstats_history_by_query;
END;
$PROC$ language plpgsql;

SELECT pg_catalog.pg_extension_config_dump('powaqualstats_statements','');
SELECT pg_catalog.pg_extension_config_dump('powaqualstats_history_by_query','');
SELECT pg_catalog.pg_extension_config_dump('powaqualstats_history_by_query_current','');


INSERT INTO powa_functions VALUES ('snapshot','powaqualstats_take_statements_snaphot',false),('aggregate','powaqualstats_aggregate',false),('purge','powaqualstats_purge',false);
