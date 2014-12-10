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
#include <limits.h>
#include "postgres.h"
#include "access/hash.h"
#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parse_node.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tuplestore.h"

PG_MODULE_MAGIC;

#define PGQS_COLUMNS 14
#define PGQS_NAME_COLUMNS 7
#define PGQS_USAGE_DEALLOC_PERCENT	5	/* free this % of entries at once */
#define PGQS_CONSTANT_SIZE 80	/* Truncate constant representation at 80 */

/*---- Function declarations ----*/

void		_PG_init(void);
void		_PG_fini(void);


extern Datum pg_qualstats_reset(PG_FUNCTION_ARGS);
extern Datum pg_qualstats(PG_FUNCTION_ARGS);
extern Datum pg_qualstats_names(PG_FUNCTION_ARGS);
static Datum pg_qualstats_common(PG_FUNCTION_ARGS, bool include_names);

PG_FUNCTION_INFO_V1(pg_qualstats_reset);
PG_FUNCTION_INFO_V1(pg_qualstats);
PG_FUNCTION_INFO_V1(pg_qualstats_names);

static void pgqs_shmem_startup(void);
static void pgqs_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pgqs_ExecutorEnd(QueryDesc *queryDesc);

static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static uint32 pgqs_hash_fn(const void *key, Size keysize);




static int	pgqs_max;			/* max # statements to track */
static bool pgqs_track_pgcatalog;		/* track queries on pg_catalog */
static bool pgqs_resolve_oids;	/* resolve oids */



/*---- Data structures declarations ----*/
typedef struct pgqsSharedState
{
#if PG_VERSION_NUM >= 90400
	LWLock		*lock;			/* protects hashtable search/modification */
#else
	LWLockId	lock;
#endif
}	pgqsSharedState;

typedef struct pgqsHashKey
{
	Oid			userid;			/* user OID */
	Oid			dbid;			/* database OID */
	uint32		parenthash;		/* Hash of the parent AND expression if any, 0
								 * otherwise. */
	uint32		nodehash;		/* Hash of the node itself */
	int64		queryid;		/* query identifier (if set by another plugin */
}	pgqsHashKey;


typedef struct pgqsNames
{
	NameData	rolname;
	NameData	datname;
	NameData	lrelname;
	NameData	lattname;
	NameData	opname;
	NameData	rrelname;
	NameData	rattname;
}	pgqsNames;


typedef struct pgqsEntry
{
	pgqsHashKey key;
	Oid			lrelid;			/* relation OID or NULL if not var */
	AttrNumber	lattnum;		/* Attribute Number or NULL if not var */
	Oid			opoid;			/* Operator OID */
	Oid			rrelid;			/* relation OID or NULL if not var */
	AttrNumber	rattnum;		/* Attribute Number or NULL if not var */
	char		constvalue[PGQS_CONSTANT_SIZE]; /* Textual representation of
												 * the right hand constant, if
												 * any */
	int64		count;
	double		filter_ratio;
	int			position;
	double		usage;
}	pgqsEntry;

typedef struct pgqsEntryWithNames
{
	pgqsEntry	entry;
	pgqsNames	names;
}	pgqsEntryWithNames;




typedef struct pgqsWalkerContext
{
	uint32		queryId;
	List		*rtable;
	uint32		parenthash;
	int64		count;
	double		filter_ratio;
}	pgqsWalkerContext;


static bool pgqs_whereclause_tree_walker(Node *node, pgqsWalkerContext * query);
static pgqsEntry *pgqs_process_opexpr(OpExpr *expr, pgqsWalkerContext * context);
static pgqsEntry *pgqs_process_scalararrayopexpr(ScalarArrayOpExpr *expr, pgqsWalkerContext * context);
static void pgqs_collectNodeStats(PlanState *planstate, List *ancestors, pgqsWalkerContext * context);
static void pgqs_collectMemberNodeStats(List *plans, PlanState **planstates, List *ancestors, pgqsWalkerContext * context);
static void pgqs_collectSubPlanStats(List *plans, List *ancestors, pgqsWalkerContext * context);
static uint32 hashExpr(Expr *expr, pgqsWalkerContext *context);
static void exprRepr(Expr *expr, StringInfo buffer, pgqsWalkerContext *context);


static void pgqs_entry_dealloc(void);
static void pgqs_fillnames(pgqsEntryWithNames * entry);

static Size pgqs_memsize(void);


/* Global Hash */
static HTAB *pgqs_hash = NULL;
static pgqsSharedState *pgqs = NULL;



void
_PG_init(void)
{
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgqs_ExecutorStart;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pgqs_ExecutorEnd;
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
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	RequestAddinShmemSpace(pgqs_memsize());

}

void
_PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorEnd_hook = prev_ExecutorEnd;
}


void
pgqs_fillnames(pgqsEntryWithNames * entry)
{
	HeapTuple	tp;

	namestrcpy(&(entry->names.rolname), GetUserNameFromId(entry->entry.key.userid));
	namestrcpy(&(entry->names.datname), get_database_name(entry->entry.key.dbid));
	if (entry->entry.lrelid != InvalidOid)
	{
		tp = SearchSysCache1(RELOID, ObjectIdGetDatum(entry->entry.lrelid));
		if (!HeapTupleIsValid(tp))
		{
			elog(ERROR, "Invalid lreloid");
		}
		namecpy(&(entry->names.lrelname), &(((Form_pg_class) GETSTRUCT(tp))->relname));
		ReleaseSysCache(tp);
		tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(entry->entry.lrelid),
							 entry->entry.lattnum);
		if (!HeapTupleIsValid(tp))
		{
			elog(ERROR, "Invalid lattr");
		}
		namecpy(&(entry->names.lattname), &(((Form_pg_attribute) GETSTRUCT(tp))->attname));
		ReleaseSysCache(tp);
	}
	if (entry->entry.opoid != InvalidOid)
	{
		tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(entry->entry.opoid));
		if (!HeapTupleIsValid(tp))
		{
			elog(ERROR, "Invalid operator");
		}
		namecpy(&(entry->names.opname), &(((Form_pg_operator) GETSTRUCT(tp))->oprname));
		ReleaseSysCache(tp);
	}
	if (entry->entry.rrelid != InvalidOid)
	{
		tp = SearchSysCache1(RELOID, ObjectIdGetDatum(entry->entry.rrelid));
		if (!HeapTupleIsValid(tp))
		{
			elog(ERROR, "Invalid rreloid");
		}
		namecpy(&(entry->names.rrelname), &(((Form_pg_class) GETSTRUCT(tp))->relname));
		ReleaseSysCache(tp);
		tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(entry->entry.rrelid),
							 entry->entry.rattnum);
		if (!HeapTupleIsValid(tp))
		{
			elog(ERROR, "Invalid rattr");
		}
		namecpy(&(entry->names.rattname), &(((Form_pg_attribute) GETSTRUCT(tp))->attname));
		ReleaseSysCache(tp);
	}
}

static void
pgqs_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	/* Setup instrumentation */
	queryDesc->instrument_options |= INSTRUMENT_ROWS;
	queryDesc->instrument_options |= INSTRUMENT_BUFFERS;
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

}

static void
pgqs_ExecutorEnd(QueryDesc *queryDesc)
{
	pgqsWalkerContext *context = palloc(sizeof(pgqsWalkerContext));

	context->queryId = queryDesc->plannedstmt->queryId;
	context->rtable = queryDesc->plannedstmt->rtable;
	context->count = 0;
	context->parenthash = 0;
	context->filter_ratio = -1;
	pgqs_collectNodeStats(queryDesc->planstate, NIL, context);
	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
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
	{
		base_size = sizeof(pgqsEntryWithNames *);
	}
	else
	{
		base_size = sizeof(pgqsEntry *);
	}

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
	{
		hash_search(pgqs_hash, &entries[i]->key, HASH_REMOVE, NULL);
	}
	pfree(entries);
}

static void
pgqs_collectNodeStats(PlanState *planstate, List *ancestors, pgqsWalkerContext * context)
{
	Plan		*plan = planstate->plan;
	int64		oldcount = context->count;
	double		oldratio = context->filter_ratio;
	double 		total_filtered = 0;
	ListCell *lc;

	switch (nodeTag(plan))
	{
		case T_CteScan:
		case T_SeqScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
		case T_ValuesScan:
		case T_WorkTableScan:
		case T_ForeignScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapIndexScan:
		case T_ModifyTable:
			if (list_length(plan->qual) > 1)
			{
				context->parenthash = hashExpr((Expr*)plan->qual, context);
			}
			total_filtered = planstate->instrument->nfiltered1 + planstate->instrument->nfiltered2;
			context->count = planstate->instrument->tuplecount + planstate->instrument->ntuples + total_filtered;
			if (total_filtered == 0){
				context->filter_ratio = 0;
			} else {
				context->filter_ratio = total_filtered / context->count;
			}
			expression_tree_walker((Node *) plan->qual, pgqs_whereclause_tree_walker, context);
			context->parenthash = 0;
			context->count = oldcount;
			context->filter_ratio = oldratio;
			break;
		case T_NestLoop:
		case T_MergeJoin:
		case T_HashJoin:
			/* TODO: compute more stuff */
			break;
		default:
			break;

	}
	foreach(lc, planstate->initPlan)
	{
		SubPlanState *sps = (SubPlanState*) lfirst(lc);
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
			pgqs_collectMemberNodeStats(((ModifyTable *) plan)->plans,
								  ((ModifyTableState *) planstate)->mt_plans,
										ancestors, context);
			break;
		case T_Append:
			pgqs_collectMemberNodeStats(((Append *) plan)->appendplans,
									((AppendState *) planstate)->appendplans,
										ancestors, context);
			break;
		case T_MergeAppend:
			pgqs_collectMemberNodeStats(((MergeAppend *) plan)->mergeplans,
								((MergeAppendState *) planstate)->mergeplans,
										ancestors, context);
			break;
		case T_BitmapAnd:
			pgqs_collectMemberNodeStats(((BitmapAnd *) plan)->bitmapplans,
								 ((BitmapAndState *) planstate)->bitmapplans,
										ancestors, context);
			break;
		case T_BitmapOr:
			pgqs_collectMemberNodeStats(((BitmapOr *) plan)->bitmapplans,
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
pgqs_collectMemberNodeStats(List *plans, PlanState **planstates,
							List *ancestors, pgqsWalkerContext * context)
{
	int			nplans = list_length(plans);
	int			j;

	for (j = 0; j < nplans; j++)
		pgqs_collectNodeStats(planstates[j], ancestors, context);
}

static void
pgqs_collectSubPlanStats(List *plans, List *ancestors, pgqsWalkerContext * context)
{
	ListCell	*lst;

	foreach(lst, plans)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lst);

		pgqs_collectNodeStats(sps->planstate, ancestors, context);
	}
}

static pgqsEntry *
pgqs_process_scalararrayopexpr(ScalarArrayOpExpr *expr, pgqsWalkerContext * context)
{
	OpExpr		*op = makeNode(OpExpr);
	int			len = 0;
	pgqsEntry  *entry;
	Expr		*array = lsecond(expr->args);

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
		context->count *= len;
		entry = pgqs_process_opexpr(op, context);
	}
	return entry;
}

static void
get_const_expr(Const *constval, StringInfo buf)
{
	Oid			typoutput;
	bool		typIsVarlena;
	char		*extval;

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
		Node		*node = linitial(expr->args);
		Var			*var = NULL;
		Const		*constant = NULL;
		bool		found;
		Oid			*sreliddest = NULL;
		AttrNumber *sattnumdest = NULL;
		pgqsHashKey key;

		pgqsEntry  tempentry;
		tempentry.opoid = expr->opno;
		tempentry.lattnum = InvalidAttrNumber;
		tempentry.lrelid = InvalidOid;
		tempentry.rattnum = InvalidAttrNumber;
		tempentry.rrelid = InvalidOid;

		key.userid = GetUserId();
		key.dbid = MyDatabaseId;
		key.parenthash = context->parenthash;
		key.nodehash = hashExpr((Expr*)expr, context);
		key.queryid = context->queryId;
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
					RangeTblEntry *rte = list_nth(context->rtable, var->varno - 1);

					if (rte->rtekind == RTE_RELATION)
					{
						tempentry.lrelid = rte->relid;
						tempentry.lattnum = var->varattno;
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
				OpExpr		*temp = copyObject(expr);

				CommuteOpExpr(temp);
				node = linitial(temp->args);
				sreliddest = &(tempentry.lrelid);
				sattnumdest = &(tempentry.lattnum);
			}
		}
		else
		{

			node = lsecond(expr->args);
			sreliddest = &(tempentry.rrelid);
			sattnumdest = &(tempentry.rattnum);

		}
		switch (node->type)
		{
			case T_Var:
				var = (Var *) node;
				{
					RangeTblEntry *rte = list_nth(context->rtable, var->varno - 1);

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

			pgqsEntry * entry;
			if (!pgqs_track_pgcatalog)
			{
				HeapTuple	tp;


				if (tempentry.lrelid != InvalidOid)
				{
					tp = SearchSysCache1(RELOID, ObjectIdGetDatum(tempentry.lrelid));
					if (!HeapTupleIsValid(tp))
					{
						elog(ERROR, "Invalid reloid");
					}
					if (((Form_pg_class) GETSTRUCT(tp))->relnamespace == PG_CATALOG_NAMESPACE)
					{
						ReleaseSysCache(tp);
						return NULL;
					}
					ReleaseSysCache(tp);
				}
				if (tempentry.rrelid != InvalidOid)
				{
					tp = SearchSysCache1(RELOID, ObjectIdGetDatum(tempentry.rrelid));
					if (!HeapTupleIsValid(tp))
					{
						elog(ERROR, "Invalid reloid");
					}
					if (((Form_pg_class) GETSTRUCT(tp))->relnamespace == PG_CATALOG_NAMESPACE)
					{
						ReleaseSysCache(tp);
						return NULL;
					}
					ReleaseSysCache(tp);
				}
			}
			LWLockAcquire(pgqs->lock, LW_EXCLUSIVE);
			entry = (pgqsEntry *) hash_search(pgqs_hash, &key, HASH_ENTER, &found);
			if (!found)
			{
				memcpy(&(entry->lrelid), &(tempentry.lrelid), sizeof(pgqsEntry) - sizeof(pgqsHashKey));
				entry->count = 0;
				entry->filter_ratio = -1;
				entry->usage = 0;
				entry->position = -1;
				entry->constvalue[0] = 0;
				if (constant != NULL)
				{
					StringInfo	buf = makeStringInfo();
					get_const_expr(constant, buf);
					entry->position = constant->location;
					strncpy(entry->constvalue, buf->data, PGQS_CONSTANT_SIZE);
				}
				if (pgqs_resolve_oids)
				{
					pgqs_fillnames((pgqsEntryWithNames *) entry);
				}
				while (hash_get_num_entries(pgqs_hash) >= pgqs_max)
					pgqs_entry_dealloc();
			}
			if (entry->filter_ratio > -1)
			{
				entry->filter_ratio = ((entry->filter_ratio * entry->count) + (context->filter_ratio * context->count)) / (entry->count + context->count);
			}
			else
			{
				entry->filter_ratio = context->filter_ratio;
			}
			entry->count += context->count;
			entry->usage += context->count;
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
				BoolExpr	*boolexpr = (BoolExpr *) node;

				if (boolexpr->boolop == NOT_EXPR)
				{
					/* Skip, and do not keep track of the qual */
					uint32		previous_hash = context->parenthash;

					context->parenthash = 0;
					expression_tree_walker((Node *) boolexpr->args, pgqs_whereclause_tree_walker, context);
					context->parenthash = previous_hash;
					return false;
				}
				if (boolexpr->boolop == OR_EXPR)
				{
					context->parenthash = 0;
				}
				if ((boolexpr->boolop == AND_EXPR))
				{
					context->parenthash = hashExpr((Expr*)boolexpr, context);
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
	if (pgqs_resolve_oids)
	{
		info.entrysize = sizeof(pgqsEntryWithNames);
	}
	else
	{
		info.entrysize = sizeof(pgqsEntry);
	}
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
	pgqsEntry  *entry;
	Datum		*values;
	bool		*nulls;

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
	if (include_names)
	{
		nb_columns += PGQS_NAME_COLUMNS;
	}

	values = palloc0(sizeof(Datum) * nb_columns);
	nulls = palloc0(sizeof(bool) * nb_columns);;
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		int			i = 0;

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
		values[i++] = Int16GetDatum(entry->opoid);
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
		values[i++] = Float8GetDatumFast(entry->filter_ratio);
		if(entry->position == -1)
		{
			nulls[i++] = true;
		}
		else
		{
			values[i++] = Int32GetDatum(entry->position);
		}
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
		if (include_names)
		{
			if(pgqs_resolve_oids)
			{
				pgqsNames	names = ((pgqsEntryWithNames *) entry)->names;
				values[i++] = CStringGetTextDatum(NameStr(names.rolname));
				values[i++] = CStringGetTextDatum(NameStr(names.datname));
				values[i++] = CStringGetTextDatum(NameStr(names.lrelname));
				values[i++] = CStringGetTextDatum(NameStr(names.lattname));
				values[i++] = CStringGetTextDatum(NameStr(names.opname));
				values[i++] = CStringGetTextDatum(NameStr(names.rrelname));
				values[i++] = CStringGetTextDatum(NameStr(names.rattname));
			} else {
				for(; i < nb_columns; i++){
					nulls[i] = true;
				}
			}
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
		hash_uint32((uint32) k->nodehash) ^
		hash_uint32((uint32) k->parenthash) ^
		hash_uint32((uint32) k->queryid);
}

/*
 * Estimate shared memory space needed.
 */
static Size
pgqs_memsize(void)
{
	Size		size;

	size = MAXALIGN(sizeof(pgqsSharedState));
	if(pgqs_resolve_oids){
		size = add_size(size, hash_estimate_size(pgqs_max, sizeof(pgqsEntryWithNames)));
	} else {
		size = add_size(size, hash_estimate_size(pgqs_max, sizeof(pgqsEntry)));
	}
	return size;
}

static uint32 hashExpr(Expr* expr, pgqsWalkerContext * context){
	StringInfo buffer = makeStringInfo();
	exprRepr(expr, buffer, context);
	return hash_any((unsigned char *) buffer->data, buffer->len);

}

static void exprRepr(Expr *expr, StringInfo buffer, pgqsWalkerContext * context)
{

	ListCell *lc;
	switch(expr->type)
	{
		case T_List:
			foreach(lc, (List*) expr)
			{
				exprRepr((Expr*) lfirst(lc), buffer, context);
			}
			break;
		case T_OpExpr:
			appendStringInfo(buffer, "%d", ((OpExpr *) expr)->opno);
			exprRepr((Expr*)((OpExpr*)expr)->args, buffer, context);
			break;
		case T_Var:
			{
			Var * var = (Var*) expr;
			RangeTblEntry *rte = list_nth(context->rtable, var->varno - 1);
			if (rte->rtekind == RTE_RELATION)
			{
				appendStringInfo(buffer, "%d;%d", rte->relid, var->varattno);
			}
			else
			{
				appendStringInfo(buffer, "NORTE%d;%d", var->varno, var->varattno);
			}
			}
			break;
		case T_BoolExpr:
			appendStringInfo(buffer, "%d", ((BoolExpr*)expr)->boolop);
			exprRepr((Expr*)((BoolExpr*)expr)->args, buffer, context);
			break;
		case T_Const:
			break;
		default:
			appendStringInfoString(buffer, nodeToString(expr));
	}
}

