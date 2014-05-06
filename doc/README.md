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


Installation
------------

- Needs postgresql header files
- make && sudo make install
- Add pg_qualstats to the shared preload libraries:
```
   shared_preload_libraries = 'pg_qualstats'
```

Configuration
-------------

The following GUCs can be configured, in postgresql.conf:

- pg_qualstats.max: the maximum number of statements tracked (defaults to 20000)

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

   Example:

```
 userid | dbid  | lrelid | lattnum | opno | rrelid | rattnum | parenthash |  nodehash   | count 
--------+-------+--------+---------+------+--------+---------+------------+-------------+-------
     10 | 16384 |  74159 |       1 |   96 |        |         |          0 |  2122697166 |     5
     10 | 16384 |  74159 |       1 |   96 |        |         |          0 |   484467142 |     2
     10 | 16384 |  74159 |       1 |   96 |        |         |          0 |  1590474470 |     3
     10 | 16384 |  74156 |       1 |   96 |        |         |          0 |  1434519808 |     1
     10 | 16384 |  74156 |       1 |   96 |        |         |          0 |  1549990478 |     1
     10 | 16384 |  74156 |       1 |   96 |        |         |          0 |  -467598540 |     1
     10 | 16384 |  74156 |       1 |   96 |        |         |          0 |  2056384448 |     1
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


Todo
----

- TEST! TEST! TEST!
- Add pg_qualstats_foreignkeys for suggesting FKs (frequently joined together
  columns)
- Normalize queries to eliminate constants
- Function or example in docs on how to use pg_qualstats with
  pg_stats_statements.
