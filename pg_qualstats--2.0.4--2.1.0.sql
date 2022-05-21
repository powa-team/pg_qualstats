-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION pg_qualstats UPDATE" to load this file. \quit

CREATE OR REPLACE FUNCTION @extschema@.pg_qualstats_index_advisor (
    min_filter integer DEFAULT 1000,
    min_selectivity integer DEFAULT 30,
    forbidden_am text[] DEFAULT '{}')
    RETURNS json
AS $_$
DECLARE
    v_res json;
    v_processed bigint[] = '{}';
    v_indexes json[];
    v_unoptimised json;

    rec record;
    v_nb_processed integer = 1;

    v_ddl text;
    v_col text;
    v_cur json;
    v_qualnodeid bigint;
    v_queryid text;
    v_quals_todo bigint[];
    v_quals_done bigint[];
    v_quals_col_done text[];
    v_queryids bigint[] = '{}';
BEGIN
    -- sanity checks and default values
    SELECT coalesce(min_filter, 1000), coalesce(min_selectivity, 30),
      coalesce(forbidden_am, '{}')
    INTO min_filter, min_selectivity, forbidden_am;

    -- don't try to generate hash indexes Before pg 10, as those are only WAL
    -- logged since pg 11.
    IF pg_catalog.current_setting('server_version_num')::bigint < 100000 THEN
        forbidden_am := array_append(forbidden_am, 'hash');
    END IF;

    -- first find out unoptimizable quals
    --FOR rec IN SELECT DISTINCT qualnodeid,
    WITH src AS (SELECT DISTINCT qualnodeid,
        (coalesce(lrelid, rrelid), coalesce(lattnum, rattnum),
          opno, eval_type)::@extschema@.qual AS qual, queryid
      FROM @extschema@.pg_qualstats() q
      JOIN pg_catalog.pg_database d ON q.dbid = d.oid
      LEFT JOIN pg_catalog.pg_operator op ON op.oid = q.opno
      LEFT JOIN pg_catalog.pg_amop amop ON amop.amopopr = op.oid
      LEFT JOIN pg_catalog.pg_am am ON am.oid = amop.amopmethod
      WHERE d.datname = current_database()
       AND eval_type = 'f'
       AND coalesce(lrelid, rrelid) != 0
       AND amname IS NULL)
    SELECT pg_catalog.json_build_object(
            'quals', pg_catalog.array_agg(@extschema@.pg_qualstats_deparse_qual(qual)),
            -- be careful to generate an empty array if no queryid availiable
            'queryids', coalesce(pg_catalog.array_agg(DISTINCT queryid)
                FILTER (WHERE queryid IS NOT NULL), '{}')
        ),
        pg_catalog.array_agg(qualnodeid)
    FROM src
    INTO v_unoptimised, v_processed;

    -- The index suggestion is done in multiple iteration, by scoring for each
    -- relation containing interesting quals a path of possibly AND-ed quals
    -- that contains other possibly AND-ed quals.  Only the higher score path
    -- will be used to create an index, so we can then compute another set of
    -- paths ignoring the quals that are now optimized with an index.
    WHILE v_nb_processed > 0 LOOP
      v_nb_processed := 0;
      FOR rec IN
        -- first, find quals that seems worth to optimize along with the
        -- possible access methods, discarding any qualnode that are marked as
        -- already processed.  Also apply access method restriction.
        WITH pgqs AS (
          SELECT dbid, amname, qualid, qualnodeid,
            (coalesce(lrelid, rrelid), coalesce(lattnum, rattnum),
            opno, eval_type)::@extschema@.qual AS qual, queryid,
            round(avg(execution_count)) AS execution_count,
            sum(occurences) AS occurences,
            round(sum(nbfiltered)::numeric / sum(occurences)) AS avg_filter,
            CASE WHEN sum(execution_count) = 0
              THEN 0
              ELSE round(sum(nbfiltered::numeric) / sum(execution_count) * 100)
            END AS avg_selectivity
          FROM @extschema@.pg_qualstats() q
          JOIN pg_catalog.pg_database d ON q.dbid = d.oid
          JOIN pg_catalog.pg_operator op ON op.oid = q.opno
          JOIN pg_catalog.pg_amop amop ON amop.amopopr = op.oid
          JOIN pg_catalog.pg_am am ON am.oid = amop.amopmethod
          WHERE d.datname = current_database()
          AND eval_type = 'f'
          AND amname != ALL (forbidden_am)
          AND coalesce(lrelid, rrelid) != 0
          AND qualnodeid != ALL(v_processed)
          GROUP BY dbid, amname, qualid, qualnodeid, lrelid, rrelid,
            lattnum, rattnum, opno, eval_type, queryid
        ),
        -- apply cardinality and selectivity restrictions
        filtered AS (
          SELECT (qual).relid, amname, coalesce(qualid, qualnodeid) AS parent,
            count(*) AS weight,
            (array_agg(qualnodeid),
             array_agg(queryid)
            )::@extschema@.adv_quals AS quals
          FROM pgqs
          WHERE avg_filter >= min_filter
          AND avg_selectivity >= min_selectivity
          GROUP BY (qual).relid, amname, parent
        ),
        -- for each possibly AND-ed qual, build the list of included qualnodeid
        nodes AS (
          SELECT p.relid, p.amname, p.parent, p.quals,
            c.quals AS children
          FROM filtered p
          LEFT JOIN filtered c ON (p.quals).qualnodeids @> (c.quals).qualnodeids
            AND p.amname = c.amname
            AND p.parent != c.parent
            AND (p.quals).qualnodeids != (c.quals).qualnodeids
        ),
        -- build the "paths", which is the list of AND-ed quals that entirely
        -- contains another possibly AND-ed quals, and give a score for each
        -- path.  The scoring method used here is simply the number of
        -- columns in the quals.
        paths AS (
          SELECT DISTINCT *,
            coalesce(pg_catalog.array_length((children).qualnodeids, 1),
                     0) AS weight
          FROM nodes
          UNION
          SELECT DISTINCT p.relid, p.amname, p.parent, p.quals, c.children,
            coalesce(pg_catalog.array_length((c.children).qualnodeids, 1),
                     0) AS weight
          FROM nodes p
          JOIN nodes c ON (p.children).qualnodeids @> (c.quals).qualnodeids
            AND (c.quals).qualnodeids IS NOT NULL
            AND (c.quals).qualnodeids != (p.quals).qualnodeids
            AND p.amname = c.amname
        ),
        -- compute the final paths.
        -- The scoring method used here is simply the sum of total
        -- number of columns in each possibly AND-ed quals, so that we can
        -- later chose to create indexes that optimize as many queries as
        -- possible with as few indexes as possible.
        -- We also compute here an access method weight, so that we can later
        -- choose a btree index rather than another access method if btree is
        -- available.
        computed AS (
          SELECT relid, amname, parent, quals,
            array_agg(to_json(children) ORDER BY weight)
              FILTER (WHERE children IS NOT NULL) AS included,
            pg_catalog.array_length((quals).qualnodeids, 1)
                + sum(weight) AS path_weight,
          CASE amname WHEN 'btree' THEN 1 ELSE 2 END AS amweight
          FROM paths
          GROUP BY relid, amname, parent, quals
        ),
        -- compute a rank for each final paths, per relation.
        final AS (
          SELECT relid, amname, parent, quals, included, path_weight, amweight,
          row_number() OVER (
            PARTITION BY relid
            ORDER BY path_weight DESC, amweight) AS rownum
          FROM computed
        )
        -- and finally choose the higher rank final path for each relation.
        SELECT relid, amname, parent,
            (quals).qualnodeids as quals, (quals).queryids as queryids,
            included, path_weight
        FROM final
        WHERE rownum = 1
      LOOP
        v_nb_processed := v_nb_processed + 1;

        v_ddl := '';
        v_quals_todo := '{}';
        v_quals_done := '{}';
        v_quals_col_done := '{}';

        -- put columns from included quals, if any, first for order dependency
        IF rec.included IS NOT NULL THEN
          FOREACH v_cur IN ARRAY rec.included LOOP
            -- Direct cast from json to bigint is only possible since pg10
            FOR v_qualnodeid IN SELECT pg_catalog.json_array_elements(v_cur->'qualnodeids')::text::bigint
            LOOP
              v_quals_todo := v_quals_todo || v_qualnodeid;
            END LOOP;
          END LOOP;
        END IF;

        -- and append qual's own columns
        v_quals_todo := v_quals_todo || rec.quals;

        -- generate the index DDL
        FOREACH v_qualnodeid IN ARRAY v_quals_todo LOOP
          -- skip quals already present in the index
          CONTINUE WHEN v_quals_done @> ARRAY[v_qualnodeid];

          -- skip other quals for the same column
          v_col := @extschema@.pg_qualstats_get_idx_col(v_qualnodeid, false);
          CONTINUE WHEN v_quals_col_done @> ARRAY[v_col];

          -- mark this qual as present in a generated index so it's ignore at
          -- next round of best quals to optimize
          v_processed := pg_catalog.array_append(v_processed, v_qualnodeid);

          -- mark this qual and col as present in this index
          v_quals_done := v_quals_done || v_qualnodeid;
          v_quals_col_done := v_quals_col_done || v_col;

          -- if underlying table has been dropped, stop here
          CONTINUE WHEN coalesce(v_col, '') = '';

          -- append the column to the index
          IF v_ddl != '' THEN v_ddl := v_ddl || ', '; END IF;
          v_ddl := v_ddl || @extschema@.pg_qualstats_get_idx_col(v_qualnodeid, true);
        END LOOP;

        -- if underlying table has been dropped, skip this (broken) index
        CONTINUE WHEN coalesce(v_ddl, '') = '';

        -- generate the full CREATE INDEX ddl
        v_ddl = pg_catalog.format('CREATE INDEX ON %s USING %I (%s)',
          @extschema@.pg_qualstats_get_qualnode_rel(v_qualnodeid), rec.amname, v_ddl);

        -- get the underlyings queryid(s)
        v_queryids = rec.queryids;
        IF rec.included IS NOT NULL THEN
          FOREACH v_cur IN ARRAY rec.included LOOP
            -- Direct cast from json to bigint is only possible since pg10
            FOR v_queryid IN SELECT pg_catalog.json_array_elements(v_cur->'queryids')::text
            LOOP
              CONTINUE WHEN v_queryid = 'null';
              v_queryids := v_queryids || v_queryid::text::bigint;
            END LOOP;
          END LOOP;
        END IF;

        -- remove any duplicates
        SELECT pg_catalog.array_agg(DISTINCT v) INTO v_queryids
            FROM (SELECT unnest(v_queryids)) s(v);

        -- sanitize the queryids
        IF v_queryids IS NULL OR v_queryids = '{null}' THEN
            v_queryids = '{}';
        END IF;

        -- and finally append the index to the list of generated indexes
        v_indexes := pg_catalog.array_append(v_indexes,
            pg_catalog.json_build_object(
                'ddl', v_ddl,
                'queryids', v_queryids
            )
        );
      END LOOP;
    END LOOP;

    v_res := pg_catalog.json_build_object(
        'indexes', v_indexes,
        'unoptimised', v_unoptimised
    );

    RETURN v_res;
END;
$_$ LANGUAGE plpgsql;       /* end of pg_qualstats_index_advisor */
