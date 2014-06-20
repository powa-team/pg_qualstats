/*-------------------------------------------------------------------------
 *
 * pg_qualstats.c
 *		Track frequently used quals.
 *
 * This extension works by installing a planner hook, pgqs_planner.
 * This hook looks for every qual in the query, and stores the qual of the form:
 *		- EXPR OPERATOR CONSTANT
 *		- EXPR OPERATOR EXPR
 *
 * The implementation is heavily inspired by pg_stat_statements
 *
 * Copyright (c) 2014, Ronan Dunklau
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
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "parser/parse_node.h"
#include "parser/analyze.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"


PG_MODULE_MAGIC;

#define PGQS_COLUMNS 12
#define PGQS_USAGE_DEALLOC_PERCENT	5	/* free this % of entries at once */
#define PGQS_CONSTANT_SIZE 80	/* Truncate constant representation at 80 */

/*---- Function declarations ----*/

void		_PG_init(void);
void		_PG_fini(void);


extern Datum pg_qualstats_reset(PG_FUNCTION_ARGS);
extern Datum pg_qualstats(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_qualstats_reset);
PG_FUNCTION_INFO_V1(pg_qualstats);



static PlannedStmt *pgqs_planner(Query *query, int cursorOptions, ParamListInfo boundParams);
static void pgqs_shmem_startup(void);


static planner_hook_type prev_planner_hook = NULL;
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
	uint32		parenthash;		/* Hash of the parent AND expression if any, 0
								 * otherwise. */
	uint32		nodehash;		/* Hash of the node itself */
	int64		queryid;		/* query identifier (if set by another plugin */
}	pgqsHashKey;


typedef struct pgqsEntry
{
	pgqsHashKey key;
	char		constvalue[PGQS_CONSTANT_SIZE]; /* Textual representation of
												 * the right hand constant, if
												 * any */
	int64		count;
}	pgqsEntry;


typedef struct pgqsWalkerContext
{
	Query	   *query;
	uint32		parenthash;
}	pgqsWalkerContext;

static bool pgqs_query_tree_walker(Node *node, pgqsWalkerContext * query);
static bool pgqs_whereclause_tree_walker(Node *node, pgqsWalkerContext * query);
static pgqsEntry *pgqs_process_opexpr(OpExpr *expr, pgqsWalkerContext * context);
static pgqsEntry *pgqs_process_scalararrayopexpr(ScalarArrayOpExpr *expr, pgqsWalkerContext * context);
static void entry_dealloc(void);


/* Global Hash */
static HTAB *pgqs_hash = NULL;
static pgqsSharedState *pgqs = NULL;



void
_PG_init(void)
{
	prev_planner_hook = planner_hook;
	planner_hook = pgqs_planner;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgqs_shmem_startup;
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
}

void
_PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
	planner_hook = prev_planner_hook;
}

static PlannedStmt *
pgqs_planner(Query *parse,
			 int cursorOptions,
			 ParamListInfo boundParams)
{
	PlannedStmt *plannedstmt = NULL;
	pgqsWalkerContext *context = palloc(sizeof(pgqsWalkerContext));

	if (prev_planner_hook)
		plannedstmt = prev_planner_hook(parse, cursorOptions, boundParams);
	else
		plannedstmt = standard_planner(parse, cursorOptions, boundParams);
	context->query = parse;
	context->parenthash = 0;
	query_or_expression_tree_walker((Node *) parse, pgqs_query_tree_walker, context, 0);
	return plannedstmt;
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
	 * Sort entries by usage and deallocate PGQS_USAGE_DEALLOC_PERCENT of
	 * them. While we're scanning the table, apply the decay factor to the
	 * usage values.
	 */

	entries = palloc(hash_get_num_entries(pgqs_hash) * sizeof(pgqsEntry *));

	i = 0;
	hash_seq_init(&hash_seq, pgqs_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		entries[i++] = entry;
	}

	qsort(entries, i, sizeof(pgqsEntry *), entry_cmp);

	nvictims = Max(10, i * PGQS_USAGE_DEALLOC_PERCENT / 100);
	nvictims = Min(nvictims, i);

	for (i = 0; i < nvictims; i++)
	{
		hash_search(pgqs_hash, &entries[i]->key, HASH_REMOVE, NULL);
	}

	pfree(entries);
}


static bool
pgqs_query_tree_walker(Node *node, pgqsWalkerContext * context)
{
	char	   *noderepr;

	if (node == NULL)
	{
		return false;
	}
	switch (node->type)
	{
		case T_FromExpr:
			noderepr = nodeToString(((FromExpr *) node)->quals);
			context->parenthash = hash_any((unsigned char *) noderepr, strlen(noderepr));
			pgqs_whereclause_tree_walker(((FromExpr *) node)->quals, context);
			context->parenthash = 0;
			expression_tree_walker((Node *) ((FromExpr *) node)->fromlist, pgqs_query_tree_walker, context);
			break;
		case T_JoinExpr:
			noderepr = nodeToString(((JoinExpr *) node)->quals);
			context->parenthash = hash_any((unsigned char *) noderepr, strlen(noderepr));
			pgqs_whereclause_tree_walker(((JoinExpr *) node)->quals, context);
			context->parenthash = 0;
			expression_tree_walker((Node *) ((JoinExpr *) node)->larg, pgqs_query_tree_walker, context);
			expression_tree_walker((Node *) ((JoinExpr *) node)->rarg, pgqs_query_tree_walker, context);
			break;
		case T_SubLink:
			query_or_expression_tree_walker(((SubLink *) node)->subselect, pgqs_query_tree_walker, context, 0);
			break;
		default:
			break;
	}
	return false;
}


static pgqsEntry *
pgqs_process_scalararrayopexpr(ScalarArrayOpExpr *expr, pgqsWalkerContext * context)
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

	getTypeOutputInfo(constval->consttype,
					  &typoutput, &typIsVarlena);

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
				 * 'NaN').	Note that strtod() and friends might accept NaN,
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


static pgqsEntry *
pgqs_process_opexpr(OpExpr *expr, pgqsWalkerContext * context)
{
	if (list_length(expr->args) == 2)
	{
		Node	   *node = linitial(expr->args);
		Var		   *var = NULL;
		Const	   *constant = NULL;
		bool		found;
		Oid		   *sreliddest = NULL;
		AttrNumber *sattnumdest = NULL;
		char	   *noderepr = nodeToString(expr);
		pgqsHashKey key;

		key.userid = GetUserId();
		key.dbid = MyDatabaseId;
		key.opoid = expr->opno;
		key.lattnum = InvalidAttrNumber;
		key.lrelid = InvalidOid;
		key.rattnum = InvalidAttrNumber;
		key.rrelid = InvalidOid;
		key.parenthash = context->parenthash;
		key.nodehash = hash_any((unsigned char *) noderepr, strlen(noderepr));
		key.queryid = context->query->queryId;
		if (IsA(node, RelabelType))
		{
			node = (Node *) ((RelabelType *) node)->arg;
		}
		switch (node->type)
		{
			case T_Var:
				var = (Var *) node;
				{
					/* Register it */
					RangeTblEntry *rte = list_nth(context->query->rtable, var->varno - 1);

					if (rte->rtekind == RTE_RELATION)
					{
						key.lrelid = rte->relid;
						key.lattnum = var->varattno;
					}
				}
				break;
			case T_Const:
				constant = (Const *) node;
				break;
			default:
				break;
		}
		/* If the operator can be commuted, look at it */
		if (var == NULL)
		{
			if (OidIsValid(get_commutator(expr->opno)))
			{
				OpExpr	   *temp = copyObject(expr);

				CommuteOpExpr(temp);
				node = linitial(temp->args);
				sreliddest = &(key.lrelid);
				sattnumdest = &(key.lattnum);
			}
		}
		else
		{

			node = lsecond(expr->args);
			sreliddest = &(key.rrelid);
			sattnumdest = &(key.rattnum);

		}
		switch (node->type)
		{
			case T_Var:
				var = (Var *) node;
				{
					RangeTblEntry *rte = list_nth(context->query->rtable, var->varno - 1);

					*sreliddest = rte->relid;
					*sattnumdest = var->varattno;
				}
				break;
			case T_Const:
				constant = (Const *) node;
				break;
			default:
				break;
		}
		if (var != NULL)
		{
			pgqsEntry  *entry;

			LWLockAcquire(pgqs->lock, LW_EXCLUSIVE);
			entry = (pgqsEntry *) hash_search(pgqs_hash, &key, HASH_ENTER, &found);
			if (!found)
			{
				entry->count = 0;
				if (constant != NULL)
				{
					StringInfo	buf = makeStringInfo();

					get_const_expr(constant, buf);
					strncpy(entry->constvalue, buf->data, PGQS_CONSTANT_SIZE);
				}
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
pgqs_whereclause_tree_walker(Node *node, pgqsWalkerContext * context)
{
	if (node == NULL)
	{
		return false;
	}
	switch (node->type)
	{
		case T_BoolExpr:
			{
				BoolExpr   *boolexpr = (BoolExpr *) node;

				if (boolexpr->boolop == NOT_EXPR)
				{
					/* Skip, and do not keep track of the qual */
					uint32		previous_hash = context->parenthash;

					context->parenthash = 0;
					/* Trampoline to pgqs_query_tree_walker, looking for */
					/* subqueries */
					expression_tree_walker(node, pgqs_query_tree_walker, context);
					context->parenthash = previous_hash;
					return false;
				}
				if (boolexpr->boolop == OR_EXPR)
				{
					context->parenthash = 0;
				}
				if ((boolexpr->boolop == AND_EXPR))
				{
					char	   *noderepr = nodeToString(node);

					context->parenthash = hash_any((unsigned char *) noderepr, strlen(noderepr));
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
		if (entry->key.parenthash == 0)
		{
			nulls[i++] = true;
		}
		else
		{
			values[i++] = UInt32GetDatum(entry->key.parenthash);
		}
		values[i++] = UInt32GetDatum(entry->key.nodehash);
		values[i++] = Int64GetDatumFast(entry->count);
		if (entry->key.queryid == 0)
		{
			nulls[i++] = true;
		}
		else
		{
			values[i++] = Int64GetDatum(entry->key.queryid);
		}
		if (entry->constvalue)
		{

			values[i++] = CStringGetTextDatum(strdup(entry->constvalue));
		}
		else
		{
			nulls[i++] = true;
		}
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
