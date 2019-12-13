/*-------------------------------------------------------------------------
 *
 * pg_qualstats.c
 *		Track frequently used quals.
 *
 * This extension works by installing a hooks on executor.
 * The ExecutorStart hook will enable some instrumentation for the
 * queries (INSTRUMENT_ROWS and INSTRUMENT_BUFFERS).
 *
 * The ExecutorEnd hook will look for every qual in the query, and
 * stores the quals of the form:
 *		- EXPR OPERATOR CONSTANT
 *		- EXPR OPERATOR EXPR
 *
 * If pg_stat_statements is available, the statistics will be
 * aggregated by queryid, and a not-normalized statement will be
 * stored for each different queryid. This can allow third part tools
 * to do some work on a real query easily.
 *
 * The implementation is heavily inspired by pg_stat_statements
 *
 * Copyright (c) 2014,2017 Ronan Dunklau
 * Copyright (c) 2018, The Powa-Team
 *-------------------------------------------------------------------------
 */
#include <limits.h>
#include <math.h>
#include "postgres.h"
#include "access/hash.h"
#include "access/htup_details.h"
#if PG_VERSION_NUM >= 90600
#include "access/parallel.h"
#endif
#if PG_VERSION_NUM >= 100000 && PG_VERSION_NUM < 110000
#include "catalog/pg_authid.h"
#endif
#if PG_VERSION_NUM >= 110000
#include "catalog/pg_authid_d.h"
#endif
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "fmgr.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "parser/analyze.h"
#include "parser/parse_node.h"
#include "parser/parsetree.h"
#include "postmaster/autovacuum.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#if PG_VERSION_NUM >= 100000
#include "storage/shmem.h"
#endif
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"

PG_MODULE_MAGIC;

#define PGQS_COLUMNS 26			/* number of columns in pg_qualstats  SRF */
#define PGQS_NAME_COLUMNS 7		/* number of column added when using
								 * pg_qualstats_column SRF */
#define PGQS_USAGE_DEALLOC_PERCENT	5	/* free this % of entries at once */
#define PGQS_MAX_LOCAL_ENTRIES	(pgqs_max * 0.2)	/* do not track more of
													 * 20% of possible entries
													 * in shared mem */
#define PGQS_CONSTANT_SIZE 80	/* Truncate constant representation at 80 */

#define PGQS_FLAGS (INSTRUMENT_ROWS|INSTRUMENT_BUFFERS)

#define PGQS_RATIO	0
#define PGQS_NUM	1

/*---- Function declarations ----*/

void		_PG_init(void);
void		_PG_fini(void);

extern Datum pg_qualstats_reset(PG_FUNCTION_ARGS);
extern Datum pg_qualstats(PG_FUNCTION_ARGS);
extern Datum pg_qualstats_names(PG_FUNCTION_ARGS);
static Datum pg_qualstats_common(PG_FUNCTION_ARGS, bool include_names);
extern Datum pg_qualstats_example_query(PG_FUNCTION_ARGS);
extern Datum pg_qualstats_example_queries(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_qualstats_reset);
PG_FUNCTION_INFO_V1(pg_qualstats);
PG_FUNCTION_INFO_V1(pg_qualstats_names);
PG_FUNCTION_INFO_V1(pg_qualstats_example_query);
PG_FUNCTION_INFO_V1(pg_qualstats_example_queries);

static void pgqs_shmem_startup(void);
static void pgqs_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pgqs_ExecutorRun(QueryDesc *queryDesc,
				 ScanDirection direction,
#if PG_VERSION_NUM >= 90600
				 uint64 count
#else
				 long count
#endif
#if PG_VERSION_NUM >= 100000
				 , bool execute_once
#endif
);
static void pgqs_ExecutorFinish(QueryDesc *queryDesc);
static void pgqs_ExecutorEnd(QueryDesc *queryDesc);

static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static uint32 pgqs_hash_fn(const void *key, Size keysize);

#if PG_VERSION_NUM < 90500
static uint32 pgqs_uint32_hashfn(const void *key, Size keysize);
#endif

static int	pgqs_query_size;
static int	pgqs_max;			/* max # statements to track */
static bool pgqs_track_pgcatalog;	/* track queries on pg_catalog */
static bool pgqs_resolve_oids;	/* resolve oids */
static bool pgqs_enabled;
static bool pgqs_track_constants;
static double pgqs_sample_rate;
static int	pgqs_min_err_ratio;
static int	pgqs_min_err_num;
static int	query_is_sampled;	/* Is the current query sampled, per backend */
static int	nesting_level = 0;	/* Current nesting depth of ExecutorRun calls */
static bool pgqs_assign_sample_rate_check_hook(double *newval, void **extra, GucSource source);
#if PG_VERSION_NUM > 90600
static void pgqs_set_query_sampled(bool sample);
#endif
static bool pgqs_is_query_sampled(void);


/*---- Data structures declarations ----*/
typedef struct pgqsSharedState
{
#if PG_VERSION_NUM >= 90400
	LWLock	   *lock;			/* protects counters hashtable
								 * search/modification */
	LWLock	   *querylock;		/* protects query hashtable
								 * search/modification */
#else
	LWLockId	lock;			/* protects counters hashtable
								 * search/modification */
	LWLockId	querylock;		/* protects query hashtable
								 * search/modification */
#endif
#if PG_VERSION_NUM >= 90600
	LWLock	   *sampledlock;	/* protects sampled array search/modification */
	bool		sampled[FLEXIBLE_ARRAY_MEMBER]; /* should we sample this
												 * query? */
#endif
} pgqsSharedState;

/* Since cff440d368, queryid becomes a uint64 internally. */

#if PG_VERSION_NUM >= 110000
typedef uint64 pgqs_queryid;
#else
typedef uint32 pgqs_queryid;
#endif

typedef struct pgqsHashKey
{
	Oid			userid;			/* user OID */
	Oid			dbid;			/* database OID */
	pgqs_queryid queryid;		/* query identifier (if set by another plugin */
	uint32		uniquequalnodeid;	/* Hash of the const */
	uint32		uniquequalid;	/* Hash of the parent, including the consts */
	char		evaltype;		/* Evaluation type. Can be 'f' to mean a qual
								 * executed after a scan, or 'i' for an
								 * indexqual */
} pgqsHashKey;

typedef struct pgqsNames
{
	NameData	rolname;
	NameData	datname;
	NameData	lrelname;
	NameData	lattname;
	NameData	opname;
	NameData	rrelname;
	NameData	rattname;
} pgqsNames;

typedef struct pgqsEntry
{
	pgqsHashKey key;
	Oid			lrelid;			/* LHS relation OID or NULL if not var */
	AttrNumber	lattnum;		/* LHS attribute Number or NULL if not var */
	Oid			opoid;			/* Operator OID */
	Oid			rrelid;			/* RHS relation OID or NULL if not var */
	AttrNumber	rattnum;		/* RHS attribute Number or NULL if not var */
	char		constvalue[PGQS_CONSTANT_SIZE]; /* Textual representation of
												 * the right hand constant, if
												 * any */
	uint32		qualid;			/* Hash of the parent AND expression if any, 0
								 * otherwise. */
	uint32		qualnodeid;		/* Hash of the node itself */

	int64		count;			/* # of operator execution */
	int64		nbfiltered;		/* # of lines discarded by the operator */
	int			position;		/* content position in query text */
	double		usage;			/* # of qual execution, used for deallocation */
	double		min_err_estim[2];	/* min estimation error ratio and num */
	double		max_err_estim[2];	/* max estimation error ratio and num */
	double		mean_err_estim[2];	/* mean estimation error ratio and num */
	double		sum_err_estim[2];	/* sum of variances in estimation error
									 * ratio and num */
	int64		occurences;		/* # of qual execution, 1 per query */
} pgqsEntry;

typedef struct pgqsEntryWithNames
{
	pgqsEntry	entry;
	pgqsNames	names;
} pgqsEntryWithNames;

typedef struct pgqsQueryStringHashKey
{
	pgqs_queryid queryid;
} pgqsQueryStringHashKey;

typedef struct pgqsQueryStringEntry
{
	pgqsQueryStringHashKey key;

	/*
	 * Imperatively at the end of the struct This is actually of length
	 * query_size, which is track_activity_query_size
	 */
	char		querytext[1];
} pgqsQueryStringEntry;

/*
 * Transient state of the query tree walker - for the meaning of the counters,
 * see pgqsEntry comments.
 */
typedef struct pgqsWalkerContext
{
	pgqs_queryid queryId;
	List	   *rtable;
	PlanState  *planstate;
	PlanState  *inner_planstate;
	PlanState  *outer_planstate;
	List	   *outer_tlist;
	List	   *inner_tlist;
	List	   *index_tlist;
	uint32		qualid;
	uint32		uniquequalid;	/* Hash of the parent, including the consts */
	int64		count;
	int64		nbfiltered;
	double		err_estim[2];
	int			nentries;		/* number of entries found so far */
	char		evaltype;
	const char *querytext;
} pgqsWalkerContext;


static bool pgqs_whereclause_tree_walker(Node *node, pgqsWalkerContext *query);
static pgqsEntry *pgqs_process_opexpr(OpExpr *expr, pgqsWalkerContext *context);
static pgqsEntry *pgqs_process_scalararrayopexpr(ScalarArrayOpExpr *expr, pgqsWalkerContext *context);
static pgqsEntry *pgqs_process_booltest(BooleanTest *expr, pgqsWalkerContext *context);
static void pgqs_collectNodeStats(PlanState *planstate, List *ancestors, pgqsWalkerContext *context);
static void pgqs_collectMemberNodeStats(int nplans, PlanState **planstates, List *ancestors, pgqsWalkerContext *context);
static void pgqs_collectSubPlanStats(List *plans, List *ancestors, pgqsWalkerContext *context);
static uint32 hashExpr(Expr *expr, pgqsWalkerContext *context, bool include_const);
static void exprRepr(Expr *expr, StringInfo buffer, pgqsWalkerContext *context, bool include_const);
static void pgqs_set_planstates(PlanState *planstate, pgqsWalkerContext *context);
static Expr *pgqs_resolve_var(Var *var, pgqsWalkerContext *context);


static void pgqs_entry_dealloc(void);
static inline void pgqs_entry_init(pgqsEntry *entry);
static inline void pgqs_entry_copy_raw(pgqsEntry *dest, pgqsEntry *src);
static void pgqs_entry_err_estim(pgqsEntry *e, double *err_estim, int64 occurences);
static void pgqs_queryentry_dealloc(void);
static void pgqs_localentry_dealloc(int nvictims);
static void pgqs_fillnames(pgqsEntryWithNames *entry);

static Size pgqs_memsize(void);
#if PG_VERSION_NUM >= 90600
static Size pgqs_sampled_array_size(void);
#endif


/* Global Hash */
static HTAB *pgqs_hash = NULL;
static HTAB *pgqs_query_examples_hash = NULL;
static pgqsSharedState *pgqs = NULL;

/* Local Hash */
static HTAB *pgqs_localhash = NULL;



void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(ERROR, "This module can only be loaded via shared_preload_libraries");
		return;
	}

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgqs_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pgqs_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pgqs_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pgqs_ExecutorEnd;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgqs_shmem_startup;

	DefineCustomBoolVariable("pg_qualstats.enabled",
							 "Enable / Disable pg_qualstats",
							 NULL,
							 &pgqs_enabled,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_qualstats.track_constants",
							 "Enable / Disable pg_qualstats constants tracking",
							 NULL,
							 &pgqs_track_constants,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("pg_qualstats.max",
							"Sets the maximum number of statements tracked by pg_qualstats.",
							NULL,
							&pgqs_max,
							1000,
							100,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("pg_qualstats.resolve_oids",
							 "Store names alongside the oid. Eats MUCH more space!",
							 NULL,
							 &pgqs_resolve_oids,
							 false,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_qualstats.track_pg_catalog",
							 "Track quals on system catalogs too.",
							 NULL,
							 &pgqs_track_pgcatalog,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable("pg_qualstats.sample_rate",
							 "Sampling rate. 1 means every query, 0.2 means 1 in five queries",
							 NULL,
							 &pgqs_sample_rate,
							 -1,
							 -1,
							 1,
							 PGC_USERSET,
							 0,
							 pgqs_assign_sample_rate_check_hook,
							 NULL,
							 NULL);

	DefineCustomIntVariable("pg_qualstats.min_err_estimate_ratio",
							"Error estimation ratio threshold to save quals",
							NULL,
							&pgqs_min_err_ratio,
							0,
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_qualstats.min_err_estimate_num",
							"Error estimation num threshold to save quals",
							NULL,
							&pgqs_min_err_num,
							0,
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	EmitWarningsOnPlaceholders("pg_qualstats");

	parse_int(GetConfigOption("track_activity_query_size", false, false),
			  &pgqs_query_size, 0, NULL);
	RequestAddinShmemSpace(pgqs_memsize());
#if PG_VERSION_NUM >= 90600
	RequestNamedLWLockTranche("pg_qualstats", 3);
#else
	RequestAddinLWLocks(2);
#endif
}

void
_PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
}


/*
 * Check that the sample ratio is in the correct interval
 */
static bool
pgqs_assign_sample_rate_check_hook(double *newval, void **extra, GucSource source)
{
	double		val = *newval;

	if ((val < 0 && val != -1) || (val > 1))
		return false;
	if (val == -1)
		*newval = 1. / MaxConnections;
	return true;
}

#if PG_VERSION_NUM >= 90600
static void
pgqs_set_query_sampled(bool sample)
{
	/* the decisions should only be made in leader */
	Assert(!IsParallelWorker());

	/* in worker processes we need to get the info from shared memory */
	LWLockAcquire(pgqs->sampledlock, LW_EXCLUSIVE);
	pgqs->sampled[MyBackendId] = sample;
	LWLockRelease(pgqs->sampledlock);
}
#endif

static bool
pgqs_is_query_sampled(void)
{
#if PG_VERSION_NUM >= 90600
	bool		sampled;

	/* in leader we can just check the global variable */
	if (!IsParallelWorker())
		return query_is_sampled;

	/* in worker processes we need to get the info from shared memory */
	LWLockAcquire(pgqs->sampledlock, LW_SHARED);
	sampled = pgqs->sampled[ParallelMasterBackendId];
	LWLockRelease(pgqs->sampledlock);

	return sampled;
#else
	return query_is_sampled;
#endif
}

/*
 * Do catalog search to replace oids with corresponding objects name
 */
void
pgqs_fillnames(pgqsEntryWithNames *entry)
{
#if PG_VERSION_NUM >= 110000
#define GET_ATTNAME(r, a)	get_attname(r, a, false)
#else
#define GET_ATTNAME(r, a)	get_attname(r, a)
#endif

#if PG_VERSION_NUM >= 90500
	namestrcpy(&(entry->names.rolname), GetUserNameFromId(entry->entry.key.userid, true));
#else
	namestrcpy(&(entry->names.rolname), GetUserNameFromId(entry->entry.key.userid));
#endif
	namestrcpy(&(entry->names.datname), get_database_name(entry->entry.key.dbid));

	if (entry->entry.lrelid != InvalidOid)
	{
		namestrcpy(&(entry->names.lrelname),
				   get_rel_name(entry->entry.lrelid));
		namestrcpy(&(entry->names.lattname),
				   GET_ATTNAME(entry->entry.lrelid, entry->entry.lattnum));
	}

	if (entry->entry.opoid != InvalidOid)
		namestrcpy(&(entry->names.opname), get_opname(entry->entry.opoid));

	if (entry->entry.rrelid != InvalidOid)
	{
		namestrcpy(&(entry->names.rrelname),
				   get_rel_name(entry->entry.rrelid));
		namestrcpy(&(entry->names.rattname),
				   GET_ATTNAME(entry->entry.rrelid, entry->entry.rattnum));
	}
#undef GET_ATTNAME
}

/*
 * Request rows and buffers instrumentation if pgqs is enabled
 */
static void
pgqs_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	/* Setup instrumentation */
	if (pgqs_enabled)
	{
		/*
		 * For rate sampling, randomly choose top-level statement. Either all
		 * nested statements will be explained or none will.
		 */
		if (nesting_level == 0
#if PG_VERSION_NUM >= 90600
			&& (!IsParallelWorker())
#endif
			)
		{
			query_is_sampled = (random() <= (MAX_RANDOM_VALUE *
											 pgqs_sample_rate));
#if PG_VERSION_NUM >= 90600
			pgqs_set_query_sampled(query_is_sampled);
#endif
		}

		if (pgqs_is_query_sampled())
			queryDesc->instrument_options |= PGQS_FLAGS;
	}
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
pgqs_ExecutorRun(QueryDesc *queryDesc,
				 ScanDirection direction,
#if PG_VERSION_NUM >= 90600
				 uint64 count
#else
				 long count
#endif
#if PG_VERSION_NUM >= 100000
				 ,bool execute_once
#endif
)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
#if PG_VERSION_NUM >= 100000
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
#else
			prev_ExecutorRun(queryDesc, direction, count);
#endif
		else
#if PG_VERSION_NUM >= 100000
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
#else
			standard_ExecutorRun(queryDesc, direction, count);
#endif
		nesting_level--;
	}
	PG_CATCH();
	{
		nesting_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
pgqs_ExecutorFinish(QueryDesc *queryDesc)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
		nesting_level--;
	}
	PG_CATCH();
	{
		nesting_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * Save a non normalized query for the queryid if no one already exists, and
 * do all the stat collecting job
 */
static void
pgqs_ExecutorEnd(QueryDesc *queryDesc)
{
	pgqsQueryStringHashKey queryKey;
	pgqsQueryStringEntry *queryEntry;
	bool		found;

	if (pgqs_enabled && pgqs_is_query_sampled()
#if PG_VERSION_NUM >= 90600
		&& (!IsParallelWorker())
#endif

	/*
	 * multiple ExecutorStart/ExecutorEnd can be interleaved, so when sampling
	 * is activated there's no guarantee that pgqs_is_query_sampled() will
	 * only detect queries that were actually sampled (thus having the
	 * required instrumentation set up).  To avoid such cases, we double check
	 * that we have the required instrumentation set up.  That won't exactly
	 * detect the sampled queries, but that should be close enough and avoid
	 * adding to much complexity.
	 */
		&& (queryDesc->instrument_options & PGQS_FLAGS) == PGQS_FLAGS
		)
	{
		HASHCTL		info;
		pgqsEntry  *localentry;
		pgqsEntry  *newEntry;
		HASH_SEQ_STATUS local_hash_seq;
		pgqsWalkerContext *context = palloc(sizeof(pgqsWalkerContext));

		context->queryId = queryDesc->plannedstmt->queryId;
		context->rtable = queryDesc->plannedstmt->rtable;
		context->count = 0;
		context->qualid = 0;
		context->uniquequalid = 0;
		context->nbfiltered = 0;
		context->evaltype = 0;
		context->nentries = 0;
		context->querytext = queryDesc->sourceText;
		queryKey.queryid = context->queryId;

		/* keep an unormalized query example for each queryid if needed */
		if (pgqs_track_constants)
		{
			/* Lookup the hash table entry with a shared lock. */
			LWLockAcquire(pgqs->querylock, LW_SHARED);

			queryEntry = (pgqsQueryStringEntry *) hash_search_with_hash_value(pgqs_query_examples_hash, &queryKey,
																			  context->queryId,
																			  HASH_FIND, &found);

			/* Create the new entry if not present */
			if (!found)
			{
				bool		excl_found;

				/* Need exclusive lock to add a new hashtable entry - promote */
				LWLockRelease(pgqs->querylock);
				LWLockAcquire(pgqs->querylock, LW_EXCLUSIVE);

				while (hash_get_num_entries(pgqs_query_examples_hash) >= pgqs_max)
					pgqs_queryentry_dealloc();

				queryEntry = (pgqsQueryStringEntry *) hash_search_with_hash_value(pgqs_query_examples_hash, &queryKey,
																				  context->queryId,
																				  HASH_ENTER, &excl_found);

				/* Make sure it wasn't added by another backend */
				if (!excl_found)
					strncpy(queryEntry->querytext, context->querytext, pgqs_query_size);
			}

			LWLockRelease(pgqs->querylock);
		}

		/* create local hash table if it hasn't been created yet */
		if (!pgqs_localhash)
		{
			memset(&info, 0, sizeof(info));
			info.keysize = sizeof(pgqsHashKey);

			if (pgqs_resolve_oids)
				info.entrysize = sizeof(pgqsEntryWithNames);
			else
				info.entrysize = sizeof(pgqsEntry);

			info.hash = pgqs_hash_fn;

			pgqs_localhash = hash_create("pgqs_localhash",
										 50,
										 &info,
										 HASH_ELEM | HASH_FUNCTION);
		}

		/* retrieve quals informations, main work starts from here */
		pgqs_collectNodeStats(queryDesc->planstate, NIL, context);

		/* if any quals found, store them in shared memory */
		if (context->nentries)
		{
			/*
			 * Before acquiring exlusive lwlock, check if there's enough room
			 * to store local hash.  Also, do not remove more than 20% of
			 * maximum number of entries in shared memory (wether they are
			 * used or not). This should not happen since we shouldn't store
			 * that much entries in localhash in the first place.
			 */
			int			nvictims = hash_get_num_entries(pgqs_localhash) -
				PGQS_MAX_LOCAL_ENTRIES;

			if (nvictims > 0)
				pgqs_localentry_dealloc(nvictims);

			LWLockAcquire(pgqs->lock, LW_EXCLUSIVE);

			while (hash_get_num_entries(pgqs_hash) +
				   hash_get_num_entries(pgqs_localhash) >= pgqs_max)
				pgqs_entry_dealloc();

			hash_seq_init(&local_hash_seq, pgqs_localhash);
			while ((localentry = hash_seq_search(&local_hash_seq)) != NULL)
			{
				newEntry = (pgqsEntry *) hash_search(pgqs_hash, &localentry->key,
													 HASH_ENTER, &found);

				if (!found)
				{
					/* raw copy the local entry */
					pgqs_entry_copy_raw(newEntry, localentry);
				}
				else
				{
					/* only update counters value */
					newEntry->count += localentry->count;
					newEntry->nbfiltered += localentry->nbfiltered;
					newEntry->usage += localentry->usage;
					/* compute estimation error min, max, mean and variance */
					pgqs_entry_err_estim(newEntry, localentry->mean_err_estim,
										 localentry->occurences);
				}
				/* cleanup local hash */
				hash_search(pgqs_localhash, &localentry->key, HASH_REMOVE, NULL);
			}

			LWLockRelease(pgqs->lock);
		}
	}

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

/*
 * qsort comparator for sorting into increasing usage order
 */
static int
entry_cmp(const void *lhs, const void *rhs)
{
	double		l_usage = (*(pgqsEntry *const *) lhs)->usage;
	double		r_usage = (*(pgqsEntry *const *) rhs)->usage;

	if (l_usage < r_usage)
		return -1;
	else if (l_usage > r_usage)
		return +1;
	else
		return 0;
}

/*
 * Deallocate least used entries.
 * Caller must hold an exlusive lock on pgqs->lock
 */
static void
pgqs_entry_dealloc(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgqsEntry **entries;
	pgqsEntry  *entry;
	int			nvictims;
	int			i;
	int			base_size;

	/*
	 * Sort entries by usage and deallocate PGQS_USAGE_DEALLOC_PERCENT of
	 * them. While we're scanning the table, apply the decay factor to the
	 * usage values.
	 */
	if (pgqs_resolve_oids)
		base_size = sizeof(pgqsEntryWithNames *);
	else
		base_size = sizeof(pgqsEntry *);

	entries = palloc(hash_get_num_entries(pgqs_hash) * base_size);

	i = 0;
	hash_seq_init(&hash_seq, pgqs_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		entries[i++] = entry;
		entry->usage *= 0.99;
	}

	qsort(entries, i, base_size, entry_cmp);

	nvictims = Max(10, i * PGQS_USAGE_DEALLOC_PERCENT / 100);
	nvictims = Min(nvictims, i);

	for (i = 0; i < nvictims; i++)
		hash_search(pgqs_hash, &entries[i]->key, HASH_REMOVE, NULL);

	pfree(entries);
}

/* Initialize all non-key fields of the given entry. */
static inline void
pgqs_entry_init(pgqsEntry *entry)
{
	/* Note that pgqsNames if needed will be explicitly filled after this */
	memset(&(entry->lrelid), 0, sizeof(pgqsEntry) - sizeof(pgqsHashKey));
}

/* Copy non-key and non-name fields from the given entry */
static inline void
pgqs_entry_copy_raw(pgqsEntry *dest, pgqsEntry *src)
{
	/* Note that pgqsNames if needed will be explicitly filled after this */
	memcpy(&(dest->lrelid),
		   &(src->lrelid),
		   (sizeof(pgqsEntry) - sizeof(pgqsHashKey)));
}

/*
 * Accurately compute estimation error ratio and num variance using Welford's
 * method. See <http://www.johndcook.com/blog/standard_deviation/>
 * Also maintain min and max values.
 */
static void
pgqs_entry_err_estim(pgqsEntry *e, double err_estim[2], int64 occurences)
{
	e->occurences += occurences;

	for (int i = 0; i < 2; i++)
	{
		if ((e->occurences - occurences) == 0)
		{
			e->min_err_estim[i] = err_estim[i];
			e->max_err_estim[i] = err_estim[i];
			e->mean_err_estim[i] = err_estim[i];
		}
		else
		{
			double		old_err = e->mean_err_estim[i];

			e->mean_err_estim[i] +=
				(err_estim[i] - old_err) / e->occurences;
			e->sum_err_estim[i] +=
				(err_estim[i] - old_err) * (err_estim[i] - e->mean_err_estim[i]);
		}

		/* calculate min/max counters */
		if (e->min_err_estim[i] > err_estim[i])
			e->min_err_estim[i] = err_estim[i];
		if (e->max_err_estim[i] < err_estim[i])
			e->max_err_estim[i] = err_estim[i];
	}
}

/*
 * Deallocate the first example query.
 * Caller must hold an exlusive lock on pgqs->querylock
 */
static void
pgqs_queryentry_dealloc(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgqsQueryStringEntry *entry;

	hash_seq_init(&hash_seq, pgqs_query_examples_hash);
	entry = hash_seq_search(&hash_seq);

	if (entry != NULL)
		hash_search_with_hash_value(pgqs_query_examples_hash, &entry->key,
									entry->key.queryid, HASH_REMOVE, NULL);

	hash_seq_term(&hash_seq);
}

/*
 * Remove the requested number of entries from pgqs_localhash.  Since the
 * entries are all coming from the same query, remove them without any specific
 * sort.
 */
static void
pgqs_localentry_dealloc(int nvictims)
{
	pgqsEntry  *localentry;
	HASH_SEQ_STATUS local_hash_seq;
	pgqsHashKey **victims;
	bool		need_seq_term = true;
	int			i,
				ptr = 0;

	if (nvictims <= 0)
		return;

	victims = palloc(sizeof(pgqsHashKey *) * nvictims);

	hash_seq_init(&local_hash_seq, pgqs_localhash);
	while (nvictims-- >= 0)
	{
		localentry = hash_seq_search(&local_hash_seq);

		/* check if caller required too many victims */
		if (!localentry)
		{
			need_seq_term = false;
			break;
		}

		victims[ptr++] = &localentry->key;
	}

	if (need_seq_term)
		hash_seq_term(&local_hash_seq);

	for (i = 0; i < ptr; i++)
		hash_search(pgqs_localhash, victims[i], HASH_REMOVE, NULL);

	pfree(victims);
}

static void
pgqs_collectNodeStats(PlanState *planstate, List *ancestors, pgqsWalkerContext *context)
{
	Plan	   *plan = planstate->plan;
	Instrumentation *instrument = planstate->instrument;
	int64		oldcount = context->count;
	double		oldfiltered = context->nbfiltered;
	double		old_err_ratio = context->err_estim[PGQS_RATIO];
	double		old_err_num = context->err_estim[PGQS_NUM];
	double		total_filtered = 0;
	ListCell   *lc;
	List	   *parent = 0;
	List	   *indexquals = 0;
	List	   *quals = 0;

	context->planstate = planstate;

	/*
	 * We have to forcibly clean up the instrumentation state because we
	 * haven't done ExecutorEnd yet.  This is pretty grotty ...
	 */
	if (instrument)
		InstrEndLoop(instrument);

	/* Retrieve the generic quals and indexquals */
	switch (nodeTag(plan))
	{
		case T_IndexOnlyScan:
			indexquals = ((IndexOnlyScan *) plan)->indexqual;
			quals = plan->qual;
			break;
		case T_IndexScan:
			indexquals = ((IndexScan *) plan)->indexqualorig;
			quals = plan->qual;
			break;
		case T_BitmapIndexScan:
			indexquals = ((BitmapIndexScan *) plan)->indexqualorig;
			quals = plan->qual;
			break;
		case T_CteScan:
		case T_SeqScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_ValuesScan:
		case T_WorkTableScan:
		case T_ForeignScan:
		case T_ModifyTable:
			quals = plan->qual;
			break;
		case T_NestLoop:
			quals = ((NestLoop *) plan)->join.joinqual;
			break;
		case T_MergeJoin:
			quals = ((MergeJoin *) plan)->mergeclauses;
			break;
		case T_HashJoin:
			quals = ((HashJoin *) plan)->hashclauses;
			break;
		default:
			break;

	}

	pgqs_set_planstates(planstate, context);
	parent = list_union(indexquals, quals);
	if (list_length(parent) > 1)
	{
		context->uniquequalid = hashExpr((Expr *) parent, context, true);
		context->qualid = hashExpr((Expr *) parent, context, false);
	}

	total_filtered = instrument->nfiltered1 + instrument->nfiltered2;
	context->nbfiltered = total_filtered;
	context->count = instrument->tuplecount + instrument->ntuples + total_filtered;

	if (plan->plan_rows == instrument->ntuples)
	{
		context->err_estim[PGQS_RATIO] = 0;
		context->err_estim[PGQS_NUM] = 0;
	}
	else if (plan->plan_rows > instrument->ntuples)
	{
		/* XXX should use use a bigger value? */
		if (instrument->ntuples == 0)
			context->err_estim[PGQS_RATIO] = plan->plan_rows * 1.0L;
		else
			context->err_estim[PGQS_RATIO] = plan->plan_rows * 1.0L / instrument->ntuples;
		context->err_estim[PGQS_NUM] = plan->plan_rows - instrument->ntuples;
	}
	else
	{
		/* plan_rows cannot be zero */
		context->err_estim[PGQS_RATIO] = instrument->ntuples * 1.0L / plan->plan_rows;
		context->err_estim[PGQS_NUM] = instrument->ntuples - plan->plan_rows;
	}

	if (context->err_estim[PGQS_RATIO] >= pgqs_min_err_ratio &&
		context->err_estim[PGQS_NUM] >= pgqs_min_err_num)
	{
		/* Add the indexquals */
		context->evaltype = 'i';
		expression_tree_walker((Node *) indexquals,
							   pgqs_whereclause_tree_walker, context);

		/* Add the generic quals */
		context->evaltype = 'f';
		expression_tree_walker((Node *) quals, pgqs_whereclause_tree_walker,
							   context);
	}

	context->qualid = 0;
	context->uniquequalid = 0;
	context->count = oldcount;
	context->nbfiltered = oldfiltered;
	context->err_estim[PGQS_RATIO] = old_err_ratio;
	context->err_estim[PGQS_NUM] = old_err_num;

	foreach(lc, planstate->initPlan)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lc);

		pgqs_collectNodeStats(sps->planstate, ancestors, context);
	}

	/* lefttree */
	if (outerPlanState(planstate))
		pgqs_collectNodeStats(outerPlanState(planstate), ancestors, context);

	/* righttree */
	if (innerPlanState(planstate))
		pgqs_collectNodeStats(innerPlanState(planstate), ancestors, context);

	/* special child plans */
	switch (nodeTag(plan))
	{
		case T_ModifyTable:
			pgqs_collectMemberNodeStats(((ModifyTableState *) planstate)->mt_nplans,
										((ModifyTableState *) planstate)->mt_plans,
										ancestors, context);
			break;
		case T_Append:
			pgqs_collectMemberNodeStats(((AppendState *) planstate)->as_nplans,
										((AppendState *) planstate)->appendplans,
										ancestors, context);
			break;
		case T_MergeAppend:
			pgqs_collectMemberNodeStats(((MergeAppendState *) planstate)->ms_nplans,
										((MergeAppendState *) planstate)->mergeplans,
										ancestors, context);
			break;
		case T_BitmapAnd:
			pgqs_collectMemberNodeStats(((BitmapAndState *) planstate)->nplans,
										((BitmapAndState *) planstate)->bitmapplans,
										ancestors, context);
			break;
		case T_BitmapOr:
			pgqs_collectMemberNodeStats(((BitmapOrState *) planstate)->nplans,
										((BitmapOrState *) planstate)->bitmapplans,
										ancestors, context);
			break;
		case T_SubqueryScan:
			pgqs_collectNodeStats(((SubqueryScanState *) planstate)->subplan, ancestors, context);
			break;
		default:
			break;
	}

	/* subPlan-s */
	if (planstate->subPlan)
		pgqs_collectSubPlanStats(planstate->subPlan, ancestors, context);
}

static void
pgqs_collectMemberNodeStats(int nplans, PlanState **planstates,
							List *ancestors, pgqsWalkerContext *context)
{
	int			i;

	for (i = 0; i < nplans; i++)
		pgqs_collectNodeStats(planstates[i], ancestors, context);
}

static void
pgqs_collectSubPlanStats(List *plans, List *ancestors, pgqsWalkerContext *context)
{
	ListCell   *lst;

	foreach(lst, plans)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lst);

		pgqs_collectNodeStats(sps->planstate, ancestors, context);
	}
}

static pgqsEntry *
pgqs_process_scalararrayopexpr(ScalarArrayOpExpr *expr, pgqsWalkerContext *context)
{
	OpExpr	   *op = makeNode(OpExpr);
	int			len = 0;
	pgqsEntry  *entry = NULL;
	Expr	   *array = lsecond(expr->args);

	op->opno = expr->opno;
	op->opfuncid = expr->opfuncid;
	op->inputcollid = expr->inputcollid;
	op->opresulttype = BOOLOID;
	op->args = expr->args;
	switch (array->type)
	{
		case T_ArrayExpr:
			len = list_length(((ArrayExpr *) array)->elements);
			break;
		case T_Const:
			/* Const is an array. */
			{
				Const	   *arrayconst = (Const *) array;
				ArrayType  *array_type;

				if (arrayconst->constisnull)
					return NULL;

				array_type = DatumGetArrayTypeP(arrayconst->constvalue);

				if (ARR_NDIM(array_type) > 0)
					len = ARR_DIMS(array_type)[0];
			}
			break;
		default:
			break;
	}

	if (len > 0)
	{
		context->count *= len;
		entry = pgqs_process_opexpr(op, context);
	}

	return entry;
}

static pgqsEntry *
pgqs_process_booltest(BooleanTest *expr, pgqsWalkerContext *context)
{
	pgqsHashKey key;
	pgqsEntry  *entry;
	bool		found;
	Var		   *var;
	Expr	   *newexpr = NULL;
	char	   *constant;
	Oid			opoid;
	RangeTblEntry *rte;

	/* do not store more than 20% of possible entries in shared mem */
	if (context->nentries >= PGQS_MAX_LOCAL_ENTRIES)
		return NULL;

	if (IsA(expr->arg, Var))
		newexpr = pgqs_resolve_var((Var *) expr->arg, context);

	if (!(newexpr && IsA(newexpr, Var)))
		return NULL;

	var = (Var *) newexpr;
	rte = list_nth(context->rtable, var->varno - 1);
	switch (expr->booltesttype)
	{
		case IS_TRUE:
			constant = "TRUE::bool";
			opoid = BooleanEqualOperator;
			break;
		case IS_FALSE:
			constant = "FALSE::bool";
			opoid = BooleanEqualOperator;
			break;
		case IS_NOT_TRUE:
			constant = "TRUE::bool";
			opoid = BooleanNotEqualOperator;
			break;
		case IS_NOT_FALSE:
			constant = "FALSE::bool";
			opoid = BooleanNotEqualOperator;
			break;
		case IS_UNKNOWN:
			constant = "NULL::bool";
			opoid = BooleanEqualOperator;
			break;
		case IS_NOT_UNKNOWN:
			constant = "NULL::bool";
			opoid = BooleanNotEqualOperator;
			break;
		default:
			/* Bail out */
			return NULL;
	}
	memset(&key, 0, sizeof(pgqsHashKey));
	key.userid = GetUserId();
	key.dbid = MyDatabaseId;
	key.uniquequalid = context->uniquequalid;
	key.uniquequalnodeid = hashExpr((Expr *) expr, context, pgqs_track_constants);
	key.queryid = context->queryId;
	key.evaltype = context->evaltype;

	/* local hash, no lock needed */
	entry = (pgqsEntry *) hash_search(pgqs_localhash, &key, HASH_ENTER, &found);
	if (!found)
	{
		context->nentries++;

		pgqs_entry_init(entry);
		entry->qualnodeid = hashExpr((Expr *) expr, context, false);
		entry->qualid = context->qualid;
		entry->opoid = opoid;

		if (rte->rtekind == RTE_RELATION)
		{
			entry->lrelid = rte->relid;
			entry->lattnum = var->varattno;
		}

		if (pgqs_track_constants)
		{
			char	   *utf8const = (char *) pg_do_encoding_conversion((unsigned char *) constant,
																	   strlen(constant),
																	   GetDatabaseEncoding(),
																	   PG_UTF8);

			strncpy(entry->constvalue, utf8const, strlen(utf8const));
		}
		else
			memset(entry->constvalue, 0, sizeof(char) * PGQS_CONSTANT_SIZE);

		if (pgqs_resolve_oids)
			pgqs_fillnames((pgqsEntryWithNames *) entry);
	}

	entry->nbfiltered += context->nbfiltered;
	entry->count += context->count;
	entry->usage += 1;
	/* compute estimation error min, max, mean and variance */
	pgqs_entry_err_estim(entry, context->err_estim, 1);

	return entry;
}

static void
get_const_expr(Const *constval, StringInfo buf)
{
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;

	if (constval->constisnull)
	{
		/*
		 * Always label the type of a NULL constant to prevent misdecisions
		 * about type when reparsing.
		 */
		appendStringInfoString(buf, "NULL");
		appendStringInfo(buf, "::%s",
						 format_type_with_typemod(constval->consttype,
												  constval->consttypmod));
		return;
	}

	getTypeOutputInfo(constval->consttype, &typoutput, &typIsVarlena);
	extval = OidOutputFunctionCall(typoutput, constval->constvalue);

	switch (constval->consttype)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			{
				/*
				 * These types are printed without quotes unless they contain
				 * values that aren't accepted by the scanner unquoted (e.g.,
				 * 'NaN').  Note that strtod() and friends might accept NaN,
				 * so we can't use that to test.
				 *
				 * In reality we only need to defend against infinity and NaN,
				 * so we need not get too crazy about pattern matching here.
				 *
				 * There is a special-case gotcha: if the constant is signed,
				 * we need to parenthesize it, else the parser might see a
				 * leading plus/minus as binding less tightly than adjacent
				 * operators --- particularly, the cast that we might attach
				 * below.
				 */
				if (strspn(extval, "0123456789+-eE.") == strlen(extval))
				{
					if (extval[0] == '+' || extval[0] == '-')
						appendStringInfo(buf, "(%s)", extval);
					else
						appendStringInfoString(buf, extval);
				}
				else
					appendStringInfo(buf, "'%s'", extval);
			}
			break;

		case BITOID:
		case VARBITOID:
			appendStringInfo(buf, "B'%s'", extval);
			break;

		case BOOLOID:
			if (strcmp(extval, "t") == 0)
				appendStringInfoString(buf, "true");
			else
				appendStringInfoString(buf, "false");
			break;

		default:
			appendStringInfoString(buf, quote_literal_cstr(extval));
			break;
	}

	pfree(extval);

	/*
	 * For showtype == 0, append ::typename unless the constant will be
	 * implicitly typed as the right type when it is read in.
	 */
	appendStringInfo(buf, "::%s",
					 format_type_with_typemod(constval->consttype,
											  constval->consttypmod));

}

/*-----------
 * In order to avoid duplicated entries for sementically equivalent OpExpr,
 * this function returns a canonical version of the given OpExpr.
 *
 * For now, the only modification is for OpExpr with a Var and a Const, we
 * prefer the form:
 * Var operator Const
 * with the Var on the LHS.  If the expression in the opposite form and the
 * operator has a commutator, we'll commute it, otherwise fallback to the
 * original OpExpr with the Var on the RHS.
 * OpExpr of the form Var operator Var can still be redundant.
 */
static OpExpr *
pgqs_get_canonical_opexpr(OpExpr *expr, bool *commuted)
{
	if (commuted)
		*commuted = false;

	/* Only OpExpr with 2 arguments needs special processing. */
	if (list_length(expr->args) != 2)
		return expr;

	/* If the 1st argument is a Var, nothing is done */
	if (IsA(linitial(expr->args), Var))
		return expr;

	/* If the 2nd argument is a Var, commute the OpExpr if possible */
	if (IsA(lsecond(expr->args), Var) && OidIsValid(get_commutator(expr->opno)))
	{
		OpExpr	   *new = copyObject(expr);

		CommuteOpExpr(new);

		if (commuted)
			*commuted = true;

		return new;
	}

	return expr;
}

static pgqsEntry *
pgqs_process_opexpr(OpExpr *expr, pgqsWalkerContext *context)
{
	/* do not store more than 20% of possible entries in shared mem */
	if (context->nentries >= PGQS_MAX_LOCAL_ENTRIES)
		return NULL;

	if (list_length(expr->args) == 2)
	{
		bool		save_qual;
		Node	   *node;
		Var		   *var;
		Const	   *constant;
		Oid		   *sreliddest;
		AttrNumber *sattnumdest;
		pgqsEntry	tempentry;

		pgqs_entry_init(&tempentry);
		tempentry.opoid = expr->opno;

		save_qual = false;
		var = NULL;				/* will store the last Var found, if any */
		constant = NULL;		/* will store the last Constant found, if any */

		/* setup the node and LHS destination fields for the 1st argument */
		node = linitial(expr->args);
		sreliddest = &(tempentry.lrelid);
		sattnumdest = &(tempentry.lattnum);

		for (int step = 0; step < 2; step++)
		{
			if (IsA(node, RelabelType))
				node = (Node *) ((RelabelType *) node)->arg;

			if (IsA(node, Var))
				node = (Node *) pgqs_resolve_var((Var *) node, context);

			switch (node->type)
			{
				case T_Var:
					var = (Var *) node;
					{
						RangeTblEntry *rte;

						rte = list_nth(context->rtable, var->varno - 1);
						if (rte->rtekind == RTE_RELATION)
						{
							save_qual = true;
							*sreliddest = rte->relid;
							*sattnumdest = var->varattno;
						}
						else
							var = NULL;
					}
					break;
				case T_Const:
					constant = (Const *) node;
					break;
				default:
					break;
			}

			/* find the node to process for the 2nd pass */
			if (step == 0)
			{
				node = NULL;

				if (var == NULL)
				{
					bool		commuted;
					OpExpr	   *new = pgqs_get_canonical_opexpr(expr, &commuted);

					/*
					 * If the OpExpr was commuted we have to use the 1st
					 * argument of the new OpExpr, and keep using the LHS as
					 * destination fields.
					 */
					if (commuted)
					{
						Assert(sreliddest == &(tempentry.lrelid));
						Assert(sattnumdest == &(tempentry.lattnum));

						node = linitial(new->args);
					}
				}

				/*
				 * If the 1st argument was a var, or if it wasn't and the
				 * operator couldn't be commuted, use the 2nd argument and the
				 * RHS as destination fields.
				 */
				if (node == NULL)
				{
					/* simply process the next argument */
					node = lsecond(expr->args);

					/*
					 * a Var was found and stored on the LHS, so if the next
					 * node  will be stored on the RHS
					 */
					sreliddest = &(tempentry.rrelid);
					sattnumdest = &(tempentry.rattnum);
				}
			}
		}

		if (save_qual)
		{
			pgqsHashKey key;
			pgqsEntry  *entry;
			StringInfo	buf = makeStringInfo();
			bool		found;
			int			position = -1;

			/*
			 * If we don't track rels in the pg_catalog schema, lookup the
			 * schema to make sure its not pg_catalog. Otherwise, bail out.
			 */
			if (!pgqs_track_pgcatalog)
			{
				Oid			nsp;

				if (tempentry.lrelid != InvalidOid)
				{
					nsp = get_rel_namespace(tempentry.lrelid);

					Assert(OidIsValid(nsp));

					if (nsp == PG_CATALOG_NAMESPACE)
						return NULL;
				}

				if (tempentry.rrelid != InvalidOid)
				{
					nsp = get_rel_namespace(tempentry.rrelid);

					Assert(OidIsValid(nsp));

					if (nsp == PG_CATALOG_NAMESPACE)
						return NULL;
				}
			}

			if (constant != NULL && pgqs_track_constants)
			{
				get_const_expr(constant, buf);
				position = constant->location;
			}

			memset(&key, 0, sizeof(pgqsHashKey));
			key.userid = GetUserId();
			key.dbid = MyDatabaseId;
			key.uniquequalid = context->uniquequalid;
			key.uniquequalnodeid = hashExpr((Expr *) expr, context, pgqs_track_constants);
			key.queryid = context->queryId;
			key.evaltype = context->evaltype;

			/* local hash, no lock needed */
			entry = (pgqsEntry *) hash_search(pgqs_localhash, &key, HASH_ENTER,
											  &found);
			if (!found)
			{
				char	   *utf8const;
				int			len;

				context->nentries++;

				/* raw copy the temporary entry */
				pgqs_entry_copy_raw(entry, &tempentry);
				entry->position = position;
				entry->qualnodeid = hashExpr((Expr *) expr, context, false);
				entry->qualid = context->qualid;

				utf8const = (char *) pg_do_encoding_conversion((unsigned char *) buf->data,
															   strlen(buf->data),
															   GetDatabaseEncoding(),
															   PG_UTF8);
				len = strlen(utf8const);

				/*
				 * The const value can use multibyte characters, so we need to
				 * be careful when truncating the value.  Note that we need to
				 * use PG_UTF8 encoding explicitly here, as the value was just
				 * converted to this encoding.
				 */
				len = pg_encoding_mbcliplen(PG_UTF8, utf8const, len,
											PGQS_CONSTANT_SIZE - 1);

				memcpy(entry->constvalue, utf8const, len);
				entry->constvalue[len] = '\0';

				if (pgqs_resolve_oids)
					pgqs_fillnames((pgqsEntryWithNames *) entry);
			}

			entry->nbfiltered += context->nbfiltered;
			entry->count += context->count;
			entry->usage += 1;
			/* compute estimation error min, max, mean and variance */
			pgqs_entry_err_estim(entry, context->err_estim, 1);

			return entry;
		}
	}

	return NULL;
}

static bool
pgqs_whereclause_tree_walker(Node *node, pgqsWalkerContext *context)
{
	if (node == NULL)
		return false;

	switch (node->type)
	{
		case T_BoolExpr:
			{
				BoolExpr   *boolexpr = (BoolExpr *) node;

				if (boolexpr->boolop == NOT_EXPR)
				{
					/* Skip, and do not keep track of the qual */
					uint32		previous_hash = context->qualid;
					uint32		previous_uniquequalnodeid = context->uniquequalid;

					context->qualid = 0;
					context->uniquequalid = 0;
					expression_tree_walker((Node *) boolexpr->args, pgqs_whereclause_tree_walker, context);
					context->qualid = previous_hash;
					context->uniquequalid = previous_uniquequalnodeid;
					return false;
				}
				else if (boolexpr->boolop == OR_EXPR)
				{
					context->qualid = 0;
					context->uniquequalid = 0;
				}
				else if (boolexpr->boolop == AND_EXPR)
				{
					context->uniquequalid = hashExpr((Expr *) boolexpr, context, pgqs_track_constants);
					context->qualid = hashExpr((Expr *) boolexpr, context, false);
				}
				expression_tree_walker((Node *) boolexpr->args, pgqs_whereclause_tree_walker, context);
				return false;
			}
		case T_OpExpr:
			pgqs_process_opexpr((OpExpr *) node, context);
			return false;
		case T_ScalarArrayOpExpr:
			pgqs_process_scalararrayopexpr((ScalarArrayOpExpr *) node, context);
			return false;
		case T_BooleanTest:
			pgqs_process_booltest((BooleanTest *) node, context);
			return false;
		default:
			expression_tree_walker(node, pgqs_whereclause_tree_walker, context);
			return false;
	}
}

static void
pgqs_shmem_startup(void)
{
	HASHCTL		info;
	HASHCTL		queryinfo;
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
	pgqs = NULL;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	pgqs = ShmemInitStruct("pg_qualstats",
						   (sizeof(pgqsSharedState)
#if PG_VERSION_NUM >= 90600
							+ pgqs_sampled_array_size()
#endif
							),
						   &found);
	memset(&info, 0, sizeof(info));
	memset(&queryinfo, 0, sizeof(queryinfo));
	info.keysize = sizeof(pgqsHashKey);
	queryinfo.keysize = sizeof(pgqsQueryStringHashKey);
	queryinfo.entrysize = sizeof(pgqsQueryStringEntry) + pgqs_query_size * sizeof(char);

	if (pgqs_resolve_oids)
		info.entrysize = sizeof(pgqsEntryWithNames);
	else
		info.entrysize = sizeof(pgqsEntry);

	info.hash = pgqs_hash_fn;
	if (!found)
	{
		/* First time through ... */
#if PG_VERSION_NUM >= 90600
		LWLockPadded *locks = GetNamedLWLockTranche("pg_qualstats");

		pgqs->lock = &(locks[0]).lock;
		pgqs->querylock = &(locks[1]).lock;
		pgqs->sampledlock = &(locks[2]).lock;
		/* mark all backends as not sampled */
		memset(pgqs->sampled, 0, pgqs_sampled_array_size());
#else
		pgqs->lock = LWLockAssign();
		pgqs->querylock = LWLockAssign();
#endif
	}
#if PG_VERSION_NUM < 90500
	queryinfo.hash = pgqs_uint32_hashfn;
#endif
	pgqs_hash = ShmemInitHash("pg_qualstatements_hash",
							  pgqs_max, pgqs_max,
							  &info,
							  HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);

	pgqs_query_examples_hash = ShmemInitHash("pg_qualqueryexamples_hash",
											 pgqs_max, pgqs_max,
											 &queryinfo,

/* On PG > 9.5, use the HASH_BLOBS optimization for uint32 keys. */
#if PG_VERSION_NUM >= 90500
											 HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE);
#else
											 HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);
#endif
	LWLockRelease(AddinShmemInitLock);
}

Datum
pg_qualstats_reset(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS hash_seq;
	pgqsEntry  *entry;

	if (!pgqs || !pgqs_hash)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_qualstats must be loaded via shared_preload_libraries")));
	}

	LWLockAcquire(pgqs->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, pgqs_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		hash_search(pgqs_hash, &entry->key, HASH_REMOVE, NULL);
	}

	LWLockRelease(pgqs->lock);
	PG_RETURN_VOID();
}

Datum
pg_qualstats_names(PG_FUNCTION_ARGS)
{
	return pg_qualstats_common(fcinfo, true);
}

Datum
pg_qualstats(PG_FUNCTION_ARGS)
{
	return pg_qualstats_common(fcinfo, false);
}

Datum
pg_qualstats_common(PG_FUNCTION_ARGS, bool include_names)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			nb_columns = PGQS_COLUMNS;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS hash_seq;
	Oid			userid = GetUserId();
	bool		is_allowed_role = false;
	pgqsEntry  *entry;
	Datum	   *values;
	bool	   *nulls;

#if PG_VERSION_NUM >= 100000
	/* Superusers or members of pg_read_all_stats members are allowed */
	is_allowed_role = is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_ALL_STATS);
#else
	 /* Superusers are allowed */
	is_allowed_role = superuser();
#endif

	if (!pgqs || !pgqs_hash)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_qualstats must be loaded via shared_preload_libraries")));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	LWLockAcquire(pgqs->lock, LW_SHARED);
	hash_seq_init(&hash_seq, pgqs_hash);

	if (include_names)
		nb_columns += PGQS_NAME_COLUMNS;

	Assert(nb_columns == tupdesc->natts);

	values = palloc0(sizeof(Datum) * nb_columns);
	nulls = palloc0(sizeof(bool) * nb_columns);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		int			i = 0;
		double		stddev_estim[2];

		memset(values, 0, sizeof(Datum) * nb_columns);
		memset(nulls, 0, sizeof(bool) * nb_columns);
		values[i++] = ObjectIdGetDatum(entry->key.userid);
		values[i++] = ObjectIdGetDatum(entry->key.dbid);

		if (entry->lattnum != InvalidAttrNumber)
		{
			values[i++] = ObjectIdGetDatum(entry->lrelid);
			values[i++] = Int16GetDatum(entry->lattnum);
		}
		else
		{
			nulls[i++] = true;
			nulls[i++] = true;
		}
		values[i++] = Int32GetDatum(entry->opoid);
		if (entry->rattnum != InvalidAttrNumber)
		{
			values[i++] = ObjectIdGetDatum(entry->rrelid);
			values[i++] = Int16GetDatum(entry->rattnum);
		}
		else
		{
			nulls[i++] = true;
			nulls[i++] = true;
		}
		if (entry->qualid == 0)
			nulls[i++] = true;
		else
			values[i++] = Int64GetDatum(entry->qualid);

		if (entry->key.uniquequalid == 0)
			nulls[i++] = true;
		else
			values[i++] = Int64GetDatum(entry->key.uniquequalid);

		values[i++] = Int64GetDatum(entry->qualnodeid);
		values[i++] = Int64GetDatum(entry->key.uniquequalnodeid);
		values[i++] = Int64GetDatum(entry->occurences);
		values[i++] = Int64GetDatum(entry->count);
		values[i++] = Int64GetDatum(entry->nbfiltered);

		for (int j = 0; j < 2; j++)
		{
			if (j == PGQS_RATIO)	/* min/max ratio are double precision */
			{
				values[i++] = Float8GetDatum(entry->min_err_estim[j]);
				values[i++] = Float8GetDatum(entry->max_err_estim[j]);
			}
			else				/* min/max num are bigint */
			{
				values[i++] = Int64GetDatum(entry->min_err_estim[j]);
				values[i++] = Int64GetDatum(entry->max_err_estim[j]);
			}
			values[i++] = Float8GetDatum(entry->mean_err_estim[j]);
			if (entry->occurences > 1)
				stddev_estim[j] = sqrt(entry->sum_err_estim[j] / entry->occurences);
			else
				stddev_estim[j] = 0.0;
			values[i++] = Float8GetDatumFast(stddev_estim[j]);
		}

		if (entry->position == -1)
			nulls[i++] = true;
		else
			values[i++] = Int32GetDatum(entry->position);

		if (entry->key.queryid == 0)
			nulls[i++] = true;
		else
			values[i++] = Int64GetDatum(entry->key.queryid);

		if (entry->constvalue[0] != '\0')
		{
			if (is_allowed_role || entry->key.userid == userid)
			{
				values[i++] = CStringGetTextDatum((char *) pg_do_encoding_conversion(
							(unsigned char *) entry->constvalue,
							strlen(entry->constvalue),
							PG_UTF8,
							GetDatabaseEncoding()));
			}
			else
			{
				/*
				 * Don't show constant text, but hint as to the reason for not
				 * doing so
				 */
				values[i++] = CStringGetTextDatum("<insufficient privilege>");
			}
		}
		else
			nulls[i++] = true;

		if (entry->key.evaltype)
			values[i++] = CharGetDatum(entry->key.evaltype);
		else
			nulls[i++] = true;

		if (include_names)
		{
			if (pgqs_resolve_oids)
			{
				pgqsNames	names = ((pgqsEntryWithNames *) entry)->names;

				values[i++] = CStringGetTextDatum(NameStr(names.rolname));
				values[i++] = CStringGetTextDatum(NameStr(names.datname));
				values[i++] = CStringGetTextDatum(NameStr(names.lrelname));
				values[i++] = CStringGetTextDatum(NameStr(names.lattname));
				values[i++] = CStringGetTextDatum(NameStr(names.opname));
				values[i++] = CStringGetTextDatum(NameStr(names.rrelname));
				values[i++] = CStringGetTextDatum(NameStr(names.rattname));
			}
			else
			{
				for (; i < nb_columns; i++)
					nulls[i] = true;
			}
		}
		Assert(i == nb_columns);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	LWLockRelease(pgqs->lock);
	tuplestore_donestoring(tupstore);
	MemoryContextSwitchTo(oldcontext);

	return (Datum) 0;
}

Datum
pg_qualstats_example_query(PG_FUNCTION_ARGS)
{
#if PG_VERSION_NUM >= 110000
	pgqs_queryid queryid = PG_GETARG_INT64(0);
#else
	pgqs_queryid queryid = PG_GETARG_UINT32(0);
#endif
	pgqsQueryStringEntry *entry;
	pgqsQueryStringHashKey queryKey;
	bool		found;

	/* don't search the hash table if track_constants isn't enabled */
	if (!pgqs_track_constants)
		PG_RETURN_NULL();

	queryKey.queryid = queryid;

	LWLockAcquire(pgqs->querylock, LW_SHARED);
	entry = hash_search_with_hash_value(pgqs_query_examples_hash, &queryKey,
										queryid, HASH_FIND, &found);
	LWLockRelease(pgqs->querylock);

	if (found)
		PG_RETURN_TEXT_P(cstring_to_text(entry->querytext));
	else
		PG_RETURN_NULL();
}

Datum
pg_qualstats_example_queries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS hash_seq;
	pgqsQueryStringEntry *entry;

	if (!pgqs || !pgqs_query_examples_hash)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_qualstats must be loaded via shared_preload_libraries")));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
						"allowed in this context")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	/* don't need to scan the hash table if track_constants isn't enabled */
	if (!pgqs_track_constants)
		return (Datum) 0;

	LWLockAcquire(pgqs->querylock, LW_SHARED);
	hash_seq_init(&hash_seq, pgqs_query_examples_hash);

	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum		values[2];
		bool		nulls[2];
		int64		queryid = entry->key.queryid;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = Int64GetDatumFast(queryid);
		values[1] = CStringGetTextDatum(entry->querytext);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

	}

	LWLockRelease(pgqs->querylock);

	return (Datum) 0;
}

/*
 * Calculate hash value for a key
 */
static uint32
pgqs_hash_fn(const void *key, Size keysize)
{
	const pgqsHashKey *k = (const pgqsHashKey *) key;

	return hash_uint32((uint32) k->userid) ^
		hash_uint32((uint32) k->dbid) ^
		hash_uint32((uint32) k->queryid) ^
		hash_uint32((uint32) k->uniquequalnodeid) ^
		hash_uint32((uint32) k->uniquequalid) ^
		hash_uint32((uint32) k->evaltype);
}

static void
pgqs_set_planstates(PlanState *planstate, pgqsWalkerContext *context)
{
	context->outer_tlist = NIL;
	context->inner_tlist = NIL;
	context->index_tlist = NIL;
	context->outer_planstate = NULL;
	context->inner_planstate = NULL;
	context->planstate = planstate;
	if (IsA(planstate, AppendState))
		context->outer_planstate = ((AppendState *) planstate)->appendplans[0];
	else if (IsA(planstate, MergeAppendState))
		context->outer_planstate = ((MergeAppendState *) planstate)->mergeplans[0];
	else if (IsA(planstate, ModifyTableState))
		context->outer_planstate = ((ModifyTableState *) planstate)->mt_plans[0];
	else
		context->outer_planstate = outerPlanState(planstate);

	if (context->outer_planstate)
		context->outer_tlist = context->outer_planstate->plan->targetlist;
	else
		context->outer_tlist = NIL;

	if (IsA(planstate, SubqueryScanState))
		context->inner_planstate = ((SubqueryScanState *) planstate)->subplan;
	else if (IsA(planstate, CteScanState))
		context->inner_planstate = ((CteScanState *) planstate)->cteplanstate;
	else
		context->inner_planstate = innerPlanState(planstate);

	if (context->inner_planstate)
		context->inner_tlist = context->inner_planstate->plan->targetlist;
	else
		context->inner_tlist = NIL;
	/* index_tlist is set only if it's an IndexOnlyScan */
	if (IsA(planstate->plan, IndexOnlyScan))
		context->index_tlist = ((IndexOnlyScan *) planstate->plan)->indextlist;
#if PG_VERSION_NUM >= 90500
	else if (IsA(planstate->plan, ForeignScan))
		context->index_tlist = ((ForeignScan *) planstate->plan)->fdw_scan_tlist;
	else if (IsA(planstate->plan, CustomScan))
		context->index_tlist = ((CustomScan *) planstate->plan)->custom_scan_tlist;
#endif
	else
		context->index_tlist = NIL;
}

static Expr *
pgqs_resolve_var(Var *var, pgqsWalkerContext *context)
{
	List	   *tlist = NULL;
	PlanState  *planstate = context->planstate;

	pgqs_set_planstates(context->planstate, context);
	switch (var->varno)
	{
		case INNER_VAR:
			tlist = context->inner_tlist;
			break;
		case OUTER_VAR:
			tlist = context->outer_tlist;
			break;
		case INDEX_VAR:
			tlist = context->index_tlist;
			break;
		default:
			return (Expr *) var;
	}
	if (tlist != NULL)
	{
		TargetEntry *entry = get_tle_by_resno(tlist, var->varattno);

		if (entry != NULL)
		{
			Var		   *newvar = (Var *) (entry->expr);

			if (var->varno == OUTER_VAR)
				pgqs_set_planstates(context->outer_planstate, context);

			if (var->varno == INNER_VAR)
				pgqs_set_planstates(context->inner_planstate, context);

			var = (Var *) pgqs_resolve_var(newvar, context);
		}
	}

	Assert(!(IsA(var, Var) && IS_SPECIAL_VARNO(var->varno)));

	/* If the result is something OTHER than a var, replace it by a constexpr */
	if (!IsA(var, Var))
	{
		Const	   *consttext;

		consttext = (Const *) makeConst(TEXTOID, -1, -1, -1, CStringGetTextDatum(nodeToString(var)), false, false);
		var = (Var *) consttext;
	}

	pgqs_set_planstates(planstate, context);

	return (Expr *) var;
}

/*
 * Estimate shared memory space needed.
 */
static Size
pgqs_memsize(void)
{
	Size		size;

	size = MAXALIGN(sizeof(pgqsSharedState));
	if (pgqs_resolve_oids)
		size = add_size(size, hash_estimate_size(pgqs_max, sizeof(pgqsEntryWithNames)));
	else
		size = add_size(size, hash_estimate_size(pgqs_max, sizeof(pgqsEntry)));

	if (pgqs_track_constants)
	{
		/*
		 * In that case, we also need an additional struct for storing
		 * non-normalized queries.
		 */
		size = add_size(size, hash_estimate_size(pgqs_max,
												 sizeof(pgqsQueryStringEntry) + pgqs_query_size * sizeof(char)));
	}
#if PG_VERSION_NUM >= 90600
	size = add_size(size, MAXALIGN(pgqs_sampled_array_size()));
#endif
	return size;
}

#if PG_VERSION_NUM >= 90600
static Size
pgqs_sampled_array_size(void)
{
	/*
	 * Parallel workers need to be sampled if their original query is also
	 * sampled.  We store in shared mem the sample state for each query,
	 * identified by their BackendId.  If need room for all possible backends,
	 * plus autovacuum launcher and workers, plus bg workers and an extra one
	 * since BackendId numerotation starts at 1.
	 */
	return (sizeof(bool) * (MaxConnections + autovacuum_max_workers + 1
							+ max_worker_processes + 1));
}
#endif

static uint32
hashExpr(Expr *expr, pgqsWalkerContext *context, bool include_const)
{
	StringInfo	buffer = makeStringInfo();

	exprRepr(expr, buffer, context, include_const);
	return hash_any((unsigned char *) buffer->data, buffer->len);

}

static void
exprRepr(Expr *expr, StringInfo buffer, pgqsWalkerContext *context, bool include_const)
{
	ListCell   *lc;

	if (expr == NULL)
		return;

	appendStringInfo(buffer, "%d-", expr->type);
	if (IsA(expr, Var))
		expr = pgqs_resolve_var((Var *) expr, context);

	switch (expr->type)
	{
		case T_List:
			foreach(lc, (List *) expr)
				exprRepr((Expr *) lfirst(lc), buffer, context, include_const);

			break;
		case T_OpExpr:
			{
				OpExpr	   *opexpr;

				opexpr = pgqs_get_canonical_opexpr((OpExpr *) expr, NULL);

				appendStringInfo(buffer, "%d", opexpr->opno);
				exprRepr((Expr *) opexpr->args, buffer, context, include_const);
				break;
			}
		case T_Var:
			{
				Var		   *var = (Var *) expr;

				RangeTblEntry *rte = list_nth(context->rtable, var->varno - 1);

				if (rte->rtekind == RTE_RELATION)
					appendStringInfo(buffer, "%d;%d", rte->relid, var->varattno);
				else
					appendStringInfo(buffer, "NORTE%d;%d", var->varno, var->varattno);
			}
			break;
		case T_BoolExpr:
			appendStringInfo(buffer, "%d", ((BoolExpr *) expr)->boolop);
			exprRepr((Expr *) ((BoolExpr *) expr)->args, buffer, context, include_const);
			break;
		case T_BooleanTest:
			if (include_const)
				appendStringInfo(buffer, "%d", ((BooleanTest *) expr)->booltesttype);

			exprRepr((Expr *) ((BooleanTest *) expr)->arg, buffer, context, include_const);
			break;
		case T_Const:
			if (include_const)
				get_const_expr((Const *) expr, buffer);
			else
				appendStringInfoChar(buffer, '?');

			break;
		case T_CoerceViaIO:
			exprRepr((Expr *) ((CoerceViaIO *) expr)->arg, buffer, context, include_const);
			appendStringInfo(buffer, "|%d", ((CoerceViaIO *) expr)->resulttype);
			break;
		case T_FuncExpr:
			appendStringInfo(buffer, "|%d(", ((FuncExpr *) expr)->funcid);
			exprRepr((Expr *) ((FuncExpr *) expr)->args, buffer, context, include_const);
			appendStringInfoString(buffer, ")");
			break;
		case T_MinMaxExpr:
			appendStringInfo(buffer, "|minmax%d(", ((MinMaxExpr *) expr)->op);
			exprRepr((Expr *) ((MinMaxExpr *) expr)->args, buffer, context, include_const);
			appendStringInfoString(buffer, ")");
			break;

		default:
			appendStringInfoString(buffer, nodeToString(expr));
	}
}

#if PG_VERSION_NUM < 90500
static uint32
pgqs_uint32_hashfn(const void *key, Size keysize)
{
	return ((pgqsQueryStringHashKey *) key)->queryid;
}
#endif
