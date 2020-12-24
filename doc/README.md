pg_qualstats
============

pg_qualstats is a PostgreSQL extension keeping statistics on predicates found
in ```WHERE``` statements and ```JOIN``` clauses.

This is useful if you want to be able to analyze what are the most-often
executed quals (predicates) on your database. The
[powa](http://powa.readthedocs.io/) project makes use of this to provide
advances index suggestions.

It also allows you to identify correlated columns, by identifying which columns
are most frequently queried together.


The extension works by looking for known patterns in queries. Currently, this
includes:

 - Binary OpExpr where at least one side is a column from a table. Whenever
   possible, the predicate will be swaped so that CONST OP VAR expressions are
   turned into VAR COMMUTED_OP CONST.
   AND and OR expression members are counted as separate entries.
   Ex: WHERE column1 = 2, WHERE column1 = column2, WHERE 3 = column3

 - ScalarArrayOpExpr where the left side is a VAR, and the right side is an
   array constant. Those will be counted one time per element in the array.
   Ex: WHERE column1 IN (2, 3) will be counted as 2 occurences for the (column1,
   '=') operator pair

 - BooleanTest where the expression is a simple boolean column reference
   Ex: WHERE column1 IS TRUE
   Please not that clauses like WHERE columns1, WHERE NOT column1 won't be
   processed by pg_qualstats (yet)

This extension also saves the first query text, as-is, for each distinct
queryid executed, with a limit of **pg_qualstats.max** entries.

Please not that the gathered data are not saved when the PostgreSQL server is
restarted.

Installation
------------

- Compatible with PostgreSQL 9.4 or later
- Needs postgresql header files
- sudo make install
- Add pg_qualstats to the shared preload libraries:

```
   shared_preload_libraries = 'pg_qualstats'
```

Configuration
-------------

The following GUCs can be configured, in postgresql.conf:

- *pg_qualstats.enabled* (boolean, default true): whether or not pg_qualstats
  should be enabled
- *pg_qualstats.track_constants* (bolean, default true): whether or not
  pg_qualstats should keep track of each constant value individually. Disabling
  this GUC will considerably reduce the number of entries necessary to keep
  track of predicates.
- *pg_qualstats.max*: the maximum number of predicated and query text tracked
  (defaults to 1000)
- *pg_qualstats.resolve_oids* (boolean, default false): whether or not
  pg_qualstats should resolve oids at query time, or juste store the oids.
  Enabling this parameter makes the data analysis much more easy, since a
  connection to the database where the query was executed won't be necessary,
  but it will eat much more space (624 bytes per entry instead of 176).
  Additionnaly, this will require some catalog lookups, which aren't free.
- *pg_qualstats.track_pg_catalog* (boolean, default false): whether or not
  pg_qualstats should compute predicates on object in pg_catalog schema.
- *pg_qualstats.sample_rate* (double, default -1): the fraction of queries that
  should be sampled. For example, 0.1 means that only one out of ten queries
  will be sampled. The default (-1) means automatic, and results in a value of 1
  / max_connections, so that statiscally, concurrency issues will be rare.

Updating the extension
----------------------

Note that as all extensions configured in shared_preload_libraries, most of the
changes are only applied once PostgreSQL is restarted with the new shared
library.  The extension object itself only provides SQL wrappers to access
internal data structures.

Also note that pg_qualstats doesn't provide extension upgrade scripts, as
there's no data saved in any of the objects created.  Therefore, you need to
first drop the extension then create it again to get the new version.

Usage
-----

- Create the extension in any database:

```
   CREATE EXTENSION pg_qualstats;
```

### Functions


The extension defines the following functions:

 - **pg_qualstats**: returns the counts for every qualifier, identified by the
   expression hash. This hash identifies each expression.
   - *userid*: oid of the user who executed the query.
   - *dbid*: oid of the database in which the query has been executed.
   - *lrelid*, *lattnum*: oid of the relation and attribute number of the VAR
     on the left hand side, if any.
   - *opno*: oid of the operator used in the expression
   - *rrelid*, *rattnum*: oid of the relation and attribute number of the VAR
     on the right hand side, if any.
   - *qualid*: normalized identifier of the parent "AND" expression, if any.
     This identifier is computed excluding the constants.  This is useful for
     identifying predicates which are used together.
   - *uniquequalid*: unique identifier of the parent "AND" expression, if any.
     This identifier is computed including the constants.
   - *qualnodeid*: normalized identifier of this simple predicate.  This
     identifier is computed excluding the constants.
   - *uniquequalnodeid*: unique identifier of this simple predicate.  This
     identifier is computed including the constats.
   - *occurences*: number of time this predicate has been invoked, ie. number
     of related query execution.
   - *execution_count*: number of time this predicate has been executed, ie.
     number of rows it processed.
   - *nbfiltered*: number of tuples this predicate discarded.
   - *min_err_estimate_ratio*: minimum selectivity estimation error ratio
   - *max_err_estimate_ratio*: maximum selectivity estimation error ratio
   - *mean_err_estimate_ratio*: mean selectivity estimation error ratio
   - *stddev_err_estimate_ratio*: standard deviation for selectivity
     estimation error ratio
   - *min_err_estimate_num*: minimum number of line for selectivity
     estimation error
   - *max_err_estimate_num*: maximum number of line for selectivity
     estimation error
   - *mean_err_estimate_num*: mean number of line for selectivity
     estimation error
   - *stddev_err_estimate_num*: standard deviation for number of line for
     selectivity estimation error
   - *constant_position*: location of the constant in the original query
     string, as reported by the parser.
   - *queryid*: if pg_stats_statements is installed, the queryid identifying
     this query, otherwise NULL.
   - *constvalue*: a string representation of the right-hand side constant, if
     any, truncated to 80 bytes. Require to be *superuser* or member of
     *pg_read_all_stats* (since PostgreSQL 10), "<insufficient privilege>"
     will be showed instead.
   - *eval_type*: evaluation type. 'f' for a predicate evaluated after a scan
     or 'i' for an index predicate.

   Example:

```
ro=# select * from pg_qualstats;
 userid │ dbid  │ lrelid │ lattnum │ opno │ rrelid │ rattnum │ qualid │ uniquequalid │ qualnodeid │ uniquequalnodeid │ occurences │ execution_count │ nbfiltered │ constant_position │ queryid │   constvalue   │ eval_type
--------+-------+--------+---------+------+--------+---------+--------+--------------+------------+------------------+------------+-----------------+------------+-------------------+---------+----------------+-----------
     10 │ 16384 │  16385 │       2 │   98 │ <NULL> │  <NULL> │ <NULL> │       <NULL> │  115075651 │       1858640877 │          1 │          100000 │      99999 │                29 │  <NULL> │ 'line 1'::text │ f
     10 │ 16384 │  16391 │       2 │   98 │  16385 │       2 │ <NULL> │       <NULL> │  497379130 │        497379130 │          1 │               0 │          0 │            <NULL> │  <NULL> │                │ f
```

 - **pg_qualstats_index_advisor(min_filter, min_selectivity, forbidden_am)**:
   Perform a global index suggestion.  By default, only predicates filtering at
   least 1000 rows and 30% of the rows in average will be considered, but this
   can be passed as parameter.  You can also provide an array of index access
   method if you want to avoid some.  For instance, on PostgreSQL 9.6 and
   prior, `hash` indexes will be ignored as those weren't crash safe yet.

   Example:

```
SELECT v
  FROM json_array_elements(
    pg_qualstats_index_advisor(min_filter => 50)->'indexes') v
  ORDER BY v::text COLLATE "C";
                               v
---------------------------------------------------------------
 "CREATE INDEX ON public.adv USING btree (id1)"
 "CREATE INDEX ON public.adv USING btree (val, id1, id2, id3)"
 "CREATE INDEX ON public.pgqs USING btree (id)"
(3 rows)

SELECT v
  FROM json_array_elements(
    pg_qualstats_index_advisor(min_filter => 50)->'unoptimised') v
  ORDER BY v::text COLLATE "C";
        v
-----------------
 "adv.val ~~* ?"
(1 row)
```

 - **pg_qualstats_deparse_qual**: format a stored predicate in the form
   `tablename.columname operatorname ?`.  This is mostly for the global index
   advisor.
 - **pg_qualstats_get_idx_col**: for the given predicate, retrieve the
   underlying column name and all the possible operator class.  This is mostly
   for the global index advisor.
 - **pg_qualstats_get_qualnode_rel**: for the given predicate, return the
   underlying table, fully qualified.  This is mostly for the global index
   advisor
 - **pg_qualstats_example_queries**: return all the stored query texts.
 - **pg_qualstats_example_query**: return the stored query text for the given
   queryid if any, otherwise NULL.
 - **pg_qualstats_names**: return all the stored query texts.
 - **pg_qualstats_reset**: reset the internal counters and forget about every
   encountered qual.

### Views

In addition to that, the extension defines some views on top of the pg_qualstats
function:

  - **pg_qualstats**: filters calls to pg_qualstats() by the current database.
  - **pg_qualstats_pretty**: performs the appropriate joins to display a readable
    aggregated form for every attribute from the pg_qualstats view

    Example:

```
ro=# select * from pg_qualstats_pretty;
 left_schema |    left_table    | left_column |   operator   | right_schema | right_table | right_column | occurences | execution_count | nbfiltered
-------------+------------------+-------------+--------------+--------------+-------------+--------------+------------+-----------------+------------
 public      | pgbench_accounts | aid         | pg_catalog.= |              |             |              |          5 |         5000000 |    4999995
 public      | pgbench_tellers  | tid         | pg_catalog.= |              |             |              |         10 |        10000000 |    9999990
 public      | pgbench_branches | bid         | pg_catalog.= |              |             |              |         10 |         2000000 |    1999990
 public      | t1               | id          | pg_catalog.= | public       | t2          | id_t1        |          1 |           10000 |       9999
```

  - **pg_qualstats_all**: sums the counts for each attribute / operator pair,
    regardless of its position as an operand (LEFT or RIGHT), grouping together
    attributes used in AND clauses.

    Example:
```
ro=# select * from pg_qualstats_all;
 dbid  | relid | userid | queryid | attnums | opno | qualid | occurences | execution_count | nbfiltered | qualnodeid
-------+-------+--------+---------+---------+------+--------+------------+-----------------+------------+------------
 16384 | 16385 |     10 |         | {2}     |   98 |        |          1 |          100000 |      99999 |  115075651
 16384 | 16391 |     10 |         | {2}     |   98 |        |          2 |               0 |          0 |  497379130
```

  - **pg_qualstats_by_query**: returns only predicates of the form VAR OPERATOR
    CONSTANT, aggregated by queryid.
