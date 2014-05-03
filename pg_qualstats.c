/*-------------------------------------------------------------------------
 *
 * pg_qualstats.c
 *		Track frequently used quals.
 *
 * This extension works by installing a hook, pgqs_post_parse_analyze.
 * This hook looks for every qual in the query, and stores the qual of the form:
 *		- EXPR OPERATOR CONSTANT
 *		- EXPR OPERATOR EXPR
 *
 * This hash table is queriable with the pg_qualstats function, which returns
 * tuples of the following form:
 *
 * - qual: string representation of the qual
 * - count: number of times this qual has been executed
 * - queryid: this field is set to the query_id added by pg_stat_statements if
 *	 it is installed, NULL otherwise.
 *
 * The implementation is heavily inspired by pg_stat_statements
 *
 * Copyright (c) 2014, Ronan Dunklau
 *
 * IDENTIFICATION
 *	  contrib/pg_stat_statements/pg_stat_statements.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/hash.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "parser/parse_node.h"
#include "parser/analyze.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "utils/array.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"


PG_MODULE_MAGIC;

#define PGQS_COLUMNS 10
#define USAGE_DEALLOC_PERCENT	5		/* free this % of entries at once */

/*---- Function declarations ----*/

void		_PG_init(void);
void		_PG_fini(void);


extern Datum pg_qualstats_reset(PG_FUNCTION_ARGS);
extern Datum pg_qualstats(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_qualstats_reset);
PG_FUNCTION_INFO_V1(pg_qualstats);



static void pgqs_post_parse_analyze(ParseState *pstate, Query *query);
static void pgqs_shmem_startup(void);


static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static uint32 pgqs_hash_fn(const void *key, Size keysize);


static int	pgqs_max;			/* max # statements to track */



/*---- Data structures declarations ----*/
typedef struct pgqsSharedState
{
#if PG_VERSION_NUM >= 90400
	LWLock	   *lock;			/* protects hashtable search/modification */
#else
	LWLockId	lock;
#endif
}	pgqsSharedState;

typedef struct pgqsHashKey
{
	Oid			userid;			/* user OID */
	Oid			dbid;			/* database OID */
	Oid			lrelid;			/* relation OID or NULL if not var */
	AttrNumber	lattnum;		/* Attribute Number or NULL if not var */
	Oid			opoid;			/* Operator OID */
	Oid			rrelid;			/* relation OID or NULL if not var */
	AttrNumber	rattnum;		/* Attribute Number or NULL if not var */
	uint32		parenthash; 	/* Hash of the parent AND expression if any, 0 otherwise. */
	uint32		nodehash; 		/* Hash of the node itself */
}	pgqsHashKey;


typedef struct pgqsEntry
{
	pgqsHashKey key;
	int64		count;
}	pgqsEntry;


typedef struct pgqsWalkerContext
{
	Query	*query;
	uint32 	parenthash;
} pgqsWalkerContext;

static bool pgqs_query_tree_walker(Node *node, pgqsWalkerContext *query);
static bool pgqs_whereclause_tree_walker(Node *node, pgqsWalkerContext *query);
static pgqsEntry *pgqs_process_opexpr(OpExpr *expr, pgqsWalkerContext *context);
static pgqsEntry *pgqs_process_scalararrayopexpr(ScalarArrayOpExpr *expr, pgqsWalkerContext *context);
static void entry_dealloc(void);


/* Global Hash */
static HTAB *pgqs_hash = NULL;
static pgqsSharedState *pgqs = NULL;



void
_PG_init(void)
{
	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = pgqs_post_parse_analyze;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgqs_shmem_startup;
	DefineCustomIntVariable("pg_qualstats.max",
			"Sets the maximum number of statements tracked by pg_qualstats.",
							NULL,
							&pgqs_max,
							20000,
							100,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);
}

void
_PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
}

static void
pgqs_post_parse_analyze(ParseState *pstate, Query *query)
{
	pgqsWalkerContext *context = palloc(sizeof(pgqsWalkerContext));
	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query);
	context->query = query;
	context->parenthash = 0;
	query_or_expression_tree_walker((Node *) query, pgqs_query_tree_walker, context, 0);
}

static int
entry_cmp(const void *lhs, const void *rhs)
{
	double		l_usage = (*(pgqsEntry * const *) lhs)->count;
	double		r_usage = (*(pgqsEntry * const *) rhs)->count;

	if (l_usage < r_usage)
		return -1;
	else if (l_usage > r_usage)
		return +1;
	else
		return 0;
}

static void
entry_dealloc(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgqsEntry **entries;
	pgqsEntry  *entry;
	int			nvictims;
	int			i;

	/*
	 * Sort entries by usage and deallocate USAGE_DEALLOC_PERCENT of them.
	 * While we're scanning the table, apply the decay factor to the usage
	 * values.
	 */

	entries = palloc(hash_get_num_entries(pgqs_hash) * sizeof(pgqsEntry *));

	i = 0;
	hash_seq_init(&hash_seq, pgqs_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		entries[i++] = entry;
	}

	qsort(entries, i, sizeof(pgqsEntry *), entry_cmp);

	nvictims = Max(10, i * USAGE_DEALLOC_PERCENT / 100);
	nvictims = Min(nvictims, i);

	for (i = 0; i < nvictims; i++)
	{
		hash_search(pgqs_hash, &entries[i]->key, HASH_REMOVE, NULL);
	}

	pfree(entries);
}


static bool
pgqs_query_tree_walker(Node *node, pgqsWalkerContext *context)
{
	if (node == NULL)
	{
		return false;
	}
	switch (node->type)
	{
		case T_FromExpr:
			pgqs_whereclause_tree_walker(((FromExpr *) node)->quals, context);
			expression_tree_walker((Node *) ((FromExpr *) node)->fromlist, pgqs_query_tree_walker, context);
			break;
		case T_JoinExpr:
			pgqs_whereclause_tree_walker(((JoinExpr *) node)->quals, context);
			expression_tree_walker((Node *) ((JoinExpr *) node)->larg, pgqs_query_tree_walker, context);
			expression_tree_walker((Node *) ((JoinExpr *) node)->rarg, pgqs_query_tree_walker, context);
			break;
		default:
			break;
	}
	return false;
}


static pgqsEntry *
pgqs_process_scalararrayopexpr(ScalarArrayOpExpr *expr, pgqsWalkerContext *context)
{
	OpExpr	   *op = makeNode(OpExpr);
	int			len = 0;
	pgqsEntry  *entry;
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
				ArrayType  *array_type = DatumGetArrayTypeP(((Const *) array)->constvalue);

				if (ARR_NDIM(array_type) > 0)
				{
					len = ARR_DIMS(array_type)[0];
				}
			}
			break;
		default:
			break;
	}
	if (len > 0)
	{
		entry = pgqs_process_opexpr(op, context);
		if (entry != NULL)
		{
			entry->count += len - 1;
		}
	}
	return entry;
}

static pgqsEntry *
pgqs_process_opexpr(OpExpr *expr, pgqsWalkerContext *context)
{
	if (list_length(expr->args) == 2)
	{
		Var		   *var = linitial(expr->args);
		bool		keep = false;
		bool		found;
		Oid		   *sreliddest = NULL;
		AttrNumber *sattnumdest = NULL;
		char *noderepr = nodeToString(expr);
		pgqsHashKey key;

		key.userid = GetUserId();
		key.dbid = MyDatabaseId;
		key.opoid = expr->opno;
		key.lattnum = InvalidAttrNumber;
		key.lrelid = InvalidOid;
		key.rattnum = InvalidAttrNumber;
		key.rrelid = InvalidOid;
		key.parenthash = context->parenthash;
		key.nodehash = hash_any((unsigned char*)noderepr, strlen(noderepr));
		if (IsA(var, RelabelType))
		{
			var = (Var *) ((RelabelType *) var)->arg;
		}
		if (IsA(var, Var))
		{
			/* Register it */
			RangeTblEntry *rte = list_nth(context->query->rtable, var->varno - 1);

			key.lrelid = rte->relid;
			key.lattnum = var->varattno;
			keep = true;
		}
		/* If the operator can be commuted, look at it */
		if (!keep)
		{
			if (OidIsValid(get_commutator(expr->opno)))
			{
				OpExpr	   *temp = copyObject(expr);

				CommuteOpExpr(temp);
				var = linitial(temp->args);
				sreliddest = &(key.lrelid);
				sattnumdest = &(key.lattnum);
			}
		}
		else
		{
			var = lsecond(expr->args);
			sreliddest = &(key.rrelid);
			sattnumdest = &(key.rattnum);

		}
		if (IsA(var, Var))
		{
			RangeTblEntry *rte = list_nth(context->query->rtable, var->varno - 1);

			keep = true;
			*sreliddest = rte->relid;
			*sattnumdest = var->varattno;
		}
		if (keep)
		{
			pgqsEntry  *entry;

			LWLockAcquire(pgqs->lock, LW_EXCLUSIVE);
			entry = (pgqsEntry *) hash_search(pgqs_hash, &key, HASH_ENTER, &found);
			if (!found)
			{
				entry->count = 0;
				while (hash_get_num_entries(pgqs_hash) >= pgqs_max)
					entry_dealloc();
			}
			entry->count += 1;
			LWLockRelease(pgqs->lock);
			return entry;
		}
	}
	return NULL;
}

static bool
pgqs_whereclause_tree_walker(Node *node, pgqsWalkerContext *context)
{
	if (node == NULL)
	{
		return false;
	}
	switch (node->type)
	{
		case T_BoolExpr:
			{
				BoolExpr * boolexpr = (BoolExpr *) node;
				if(boolexpr->boolop == NOT_EXPR)
				{
					// Skip, and do not keep track of the qual
					return false;
				}
				if(boolexpr->boolop == 	OR_EXPR)
				{
					context->parenthash = 0;
				}
				if((boolexpr->boolop == AND_EXPR))
				{
					char *noderepr = nodeToString(node);
					context->parenthash = hash_any((unsigned char*) noderepr, strlen(noderepr));
				}
				expression_tree_walker(node, pgqs_whereclause_tree_walker, context);
				return false;
			}
		case T_OpExpr:
			pgqs_process_opexpr((OpExpr *) node, context);
			return false;
		case T_ScalarArrayOpExpr:
			pgqs_process_scalararrayopexpr((ScalarArrayOpExpr *) node, context);
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
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	pgqs = NULL;
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(pgqsHashKey);
	info.entrysize = sizeof(pgqsEntry);
	info.hash = pgqs_hash_fn;
	pgqs = ShmemInitStruct("pg_qualstats",
						   sizeof(pgqsSharedState),
						   &found);
	if (!found)
	{
		/* First time through ... */
		pgqs->lock = LWLockAssign();
	}
	pgqs_hash = ShmemInitHash("pg_qualstatements_hash",
							  pgqs_max, pgqs_max,
							  &info,
							  HASH_ELEM | HASH_FUNCTION | HASH_FIXED_SIZE);
	LWLockRelease(AddinShmemInitLock);
}

Datum
pg_qualstats_reset(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS hash_seq;
	pgqsEntry  *entry;

	if (!pgqs || !pgqs_hash)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
		errmsg("pg_qualstats must be loaded via shared_preload_libraries")));
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
pg_qualstats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	HASH_SEQ_STATUS hash_seq;
	pgqsEntry  *entry;

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
	hash_seq_init(&hash_seq, pgqs_hash);
	LWLockAcquire(pgqs->lock, LW_SHARED);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum		values[PGQS_COLUMNS];
		bool		nulls[PGQS_COLUMNS];
		int			i = 0;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		values[i++] = ObjectIdGetDatum(entry->key.userid);
		values[i++] = ObjectIdGetDatum(entry->key.dbid);
		if (entry->key.lattnum != InvalidAttrNumber)
		{
			values[i++] = ObjectIdGetDatum(entry->key.lrelid);
			values[i++] = Int16GetDatum(entry->key.lattnum);
		}
		else
		{
			nulls[i++] = true;
			nulls[i++] = true;
		}
		values[i++] = Int16GetDatum(entry->key.opoid);
		if (entry->key.rattnum != InvalidAttrNumber)
		{
			values[i++] = ObjectIdGetDatum(entry->key.rrelid);
			values[i++] = Int16GetDatum(entry->key.rattnum);
		}
		else
		{
			nulls[i++] = true;
			nulls[i++] = true;
		}
		if(entry->key.parenthash == 0)
		{
			nulls[i++] = true;
		} else {
			values[i++] = Int32GetDatum(entry->key.parenthash);
		}
		values[i++] = Int32GetDatum(entry->key.nodehash);
		values[i++] = Int64GetDatumFast(entry->count);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	LWLockRelease(pgqs->lock);
	tuplestore_donestoring(tupstore);
	MemoryContextSwitchTo(oldcontext);
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
		hash_uint32((uint32) k->opoid) ^
		hash_uint32((uint32) k->lrelid) ^
		hash_uint32((uint32) k->lattnum) ^
		hash_uint32((uint32) k->rrelid) ^
		hash_uint32((uint32) k->rattnum) ^
		k->parenthash ^
		k->nodehash;
}
