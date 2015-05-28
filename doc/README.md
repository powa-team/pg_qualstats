pg_qualstats
============

pg_qualstats is a PostgreSQL extension keeping statistics on predicates found
in ```WHERE``` statements and ```JOIN``` clauses.

Most of the code is a blatant rip-off of pg_stat_statements.

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

This project is sponsored by [Dalibo](http://dalibo.com)


Installation
------------

- Compatible with PostgreSQL 9.4
- Needs postgresql header files
- sudo make install
- Add pg_qualstats to the shared preload libraries:

```
   shared_preload_libraries = 'pg_qualstats'
```

Configuration
-------------

The following GUCs can be configured, in postgresql.conf:

- pg_qualstats.enabled (boolean, default true): whether or not pg_qualstats should be enabled
- pg_qualstats.track_constants (bolean, default true): whether or not
  pg_qualstats should keep track of each constant value individually. Disabling
  this GUC will considerably reduce the number of entries necessary to keep
  track of predicates.
- pg_qualstats.max: the maximum number of statements tracked (defaults to 1000)
- pg_qualstats.resolve_oids (boolean, default false): whether or not
  pg_qualstats should resolve_oids at query time, or juste store the oids.
  Enabling this parameter makes the data analysis much more easy, since a
  connection to the database where the query was executed won't be necessary,
  but it will eat much more space (616 bytes per entry instead of 168).
  Additionnaly, this will require some catalog lookups, which aren't free.

Usage
-----

- Create the extension in any database:

```
   CREATE EXTENSION pg_qualstats;
```

### Functions


The extension defines the following functions:

 - pg_qualstats: returns the counts for every qualifier, identified by the
   expression hash. This hash identifies each expression.
   - *userid*: oid of the user who executed the query
   - *dbid*: oid of the database in which the query has been executed
   - *lrelid*, *lattnum*: oid of the relation and attribute number of the VAR on
	 the left hand side, if any
   - *rrelid*, *rattnum*: oid of the relation and attribute number of the VAR on
	 the right hand side, if any
   - *parenthash*: hash of the parent "AND" expression, if any. This is useful
	 for identifying predicates which are used together
   - *nodehash*: the predicate hash. Everything (down to constants) is
	 used to compute this hash
   - *count*: the total number of occurences of this predicate
   - *queryid*: if pg_stats_statements is installed, the queryid identifying
     this query
   - *constvalue*: a string representation of the right-hand side constant, if
     any, truncated to 80 bytes.

   Example:

```
ro=# select * from pg_qualstats;
 userid | dbid  | lrelid | lattnum | opno | rrelid | rattnum | parenthash  |  nodehash  | count | queryid | constvalue  
--------+-------+--------+---------+------+--------+---------+-------------+------------+-------+---------+-------------
     10 | 16546 |   1262 |       1 |   93 |        |         |  1167468204 | -312474735 |     1 |         | 12::integer
     10 | 16546 |        |         |  607 |   1262 |      -2 | -1449854762 | 1327480291 |     1 |         | 
```



 - pg_qualstats_reset: reset the internal counters and forget about every
   encountered qual.

### Views

In addition to that, the extension defines some views on top of the pg_qualstats
function:

  - pg_qualstats: filters calls to pg_qualstats() by the current database.

  - pg_qualstats_pretty: performs the appropriate joins to display a readable
    form for every attribute from the pg_qualstats view

    Example:
  
```
ro=# select * from pg_qualstats_pretty;
 left_schema |    left_table    | left_column |   operator   | right_schema | right_table | right_column | count 
-------------+------------------+-------------+--------------+--------------+-------------+--------------+-------
 public      | pgbench_accounts | aid         | pg_catalog.= |              |             |              |    20
 public      | pgbench_tellers  | tid         | pg_catalog.= |              |             |              |    10
 public      | pgbench_branches | bid         | pg_catalog.= |              |             |              |    10
```

  - pg_qualstats_all: sums the counts for each attribute / operator pair,
    regardless of its position as an operand (LEFT or RIGHT), grouping together
	attributes used in AND clauses.

    Example:
```
ro=# select * from pg_qualstats_all;
 relid | attnums | opno | parenthash  | count 
-------+---------+------+-------------+-------
 74150 | {1,3}   |   96 | -1878264478 |     2
 74153 | {1}     |   96 |           0 |    10
 74156 | {1}     |   96 |           0 |    20
 74159 | {1}     |   96 |           0 |    10
```

  - pg_qualstats_indexes: looks up those attributes for which an index doesn't
    exist with the attribute in first position.

  Example:
```
ro=# select * from pg_qualstats_indexes;
      relid       |          attnames           | possible_types | count 
------------------+-----------------------------+----------------+-------
 pgbench_accounts | {filler}                    | {btree,hash}   |     5
 pgbench_accounts | {bid}                       | {btree,hash}   |     2
 pgbench_accounts | {bid,filler}                | {btree,hash}   |     8
(9 rows)
```

  - pg_qualstats_by_query: returns only predicates of the form VAR OPERATOR
    CONSTANT, aggregated by queryid.
