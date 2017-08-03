package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.parser.VerdictSQLParser.Group_by_itemContext;
import edu.umich.verdict.parser.VerdictSQLParser.Join_partContext;
import edu.umich.verdict.parser.VerdictSQLParser.Order_by_expressionContext;
import edu.umich.verdict.parser.VerdictSQLParser.Select_list_elemContext;
import edu.umich.verdict.parser.VerdictSQLParser.Table_sourceContext;
import edu.umich.verdict.relation.condition.AndCond;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.OrderByExpr;
import edu.umich.verdict.relation.expr.SelectElem;
import edu.umich.verdict.util.StackTraceReader;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.TypeCasting;
import edu.umich.verdict.util.VerdictLogger;

/**
 * Base class for exact relations (and any relational operations on them).
 * @author Yongjoo Park
 *
 */
public abstract class ExactRelation extends Relation {

    public ExactRelation(VerdictContext vc) {
        super(vc);
    }

    public static ExactRelation from(VerdictContext vc, String sql) {
        VerdictSQLParser p = StringManipulations.parserOf(sql);
        RelationGen g = new RelationGen(vc);
        return g.visit(p.select_statement());
    }

    public static ExactRelation from(VerdictContext vc, VerdictSQLParser.Select_statementContext ctx) {
        RelationGen g = new RelationGen(vc);
        return g.visit(ctx);
    }

    public ExactRelation withAlias(String alias) {
        this.setAlias(alias);
        return this;
    }


    //	/**
    //	 * Returns an expression for a (possibly joined) table source.
    //	 * SingleSourceRelation: a table name
    //	 * JoinedRelation: a join expression
    //	 * FilteredRelation: a full toSql()
    //	 * ProjectedRelation: a full toSql()
    //	 * AggregatedRelation: a full toSql()
    //	 * GroupedRelation: a full toSql()
    //	 * @return
    //	 */
    //	protected abstract String getSourceExpr();

    /**
     * Returns a name for a (possibly joined) table source. It will be an alias name if the source is a derived table.
     * @return
     */
    protected abstract String getSourceName();

    //	public abstract List<SelectElem> getSelectList();


    /*
     * Projection
     */

    public ExactRelation select(List<String> elems) {
        List<SelectElem> selectElems = new ArrayList<SelectElem>();
        for (String e : elems) {
            selectElems.add(SelectElem.from(vc, e));
        }
        return new ProjectedRelation(vc, this, selectElems);
    }

    public ExactRelation select(String elems) {
        VerdictSQLParser p = StringManipulations.parserOf(elems);
        return select(p.select_list());
    }

    public ExactRelation select(VerdictSQLParser.Select_listContext ctx) {
        VerdictSQLBaseVisitor<List<SelectElem>> elemVisitor = new VerdictSQLBaseVisitor<List<SelectElem>>() {
            private List<SelectElem> selectElems = new ArrayList<SelectElem>();
            @Override
            public List<SelectElem> visitSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx) {
                selectElems.add(SelectElem.from(vc, ctx));
                return selectElems;
            }
        };
        List<SelectElem> selectElems = elemVisitor.visit(ctx);
        return new ProjectedRelation(vc, this, selectElems);
    }

    /*
     * Filtering
     */

    /**
     * Returns a relation with an extra filtering condition.
     * The immediately following filter (or where) function on the joined relation will work as a join condition.
     * @param cond
     * @return
     * @throws VerdictException
     */
    public ExactRelation filter(Cond cond) throws VerdictException {
        return new FilteredRelation(vc, this, cond);
    }

    public ExactRelation filter(String cond) throws VerdictException {
        return filter(Cond.from(vc, cond));
    }

    public ExactRelation where(String cond) throws VerdictException {
        return filter(cond);
    }

    public ExactRelation where(Cond cond) throws VerdictException {
        return filter(cond);
    }

    /*
     * Aggregation
     */

    public ExactRelation agg(Object... elems) {
        return agg(Arrays.asList(elems));
    }

    public ExactRelation agg(List<Object> elems) {
        List<SelectElem> se = new ArrayList<SelectElem>();
        for (Object e : elems) {
            if (e instanceof SelectElem) {
                se.add((SelectElem) e);
            } else {
                se.add(SelectElem.from(vc, e.toString()));
            }
        } 

        List<Expr> exprs = new ArrayList<Expr>();
        for (SelectElem elem : se) {
            exprs.add(elem.getExpr());
        }

        // add some groupbys
        if (this instanceof GroupedRelation) {
            List<Expr> groupby = ((GroupedRelation) this).getGroupby();
            for (Expr group : groupby) {
                se.add(0, new SelectElem(group));
            }
        }

        ExactRelation r = new AggregatedRelation(vc, this, exprs);
        r = new ProjectedRelation(vc, this, se);
        return r;
    }

    @Override
    public ExactRelation count() throws VerdictException {
        return agg(FuncExpr.count());
    }

    @Override
    public ExactRelation sum(String expr) throws VerdictException {
        return agg(FuncExpr.sum(Expr.from(vc, expr)));
    }

    @Override
    public ExactRelation avg(String expr) throws VerdictException {
        return agg(FuncExpr.avg(Expr.from(vc, expr)));
    }

    @Override
    public ExactRelation countDistinct(String expr) throws VerdictException {
        return agg(FuncExpr.countDistinct(Expr.from(vc, expr)));
    }

    public GroupedRelation groupby(String group) {
        String[] tokens = group.split(",");
        return groupby(Arrays.asList(tokens));
    }

    public GroupedRelation groupby(List<String> group_list) {
        List<Expr> groups = new ArrayList<Expr>();
        for (String t : group_list) {
            groups.add(Expr.from(vc, t));
        }
        return new GroupedRelation(vc, this, groups);
    }

    /*
     * Approx Aggregation
     */

    public ApproxRelation approxAgg(List<Object> elems) throws VerdictException {
        return agg(elems).approx();
    }

    public ApproxRelation approxAgg(Object... elems) throws VerdictException {
        return agg(elems).approx();
    }

    public long approxCount() throws VerdictException {
        return TypeCasting.toLong(approxAgg(FuncExpr.count()).collect().get(0).get(0));
    }

    public double approxSum(Expr expr) throws VerdictException {
        return TypeCasting.toDouble(approxAgg(FuncExpr.sum(expr)).collect().get(0).get(0));
    }

    public double approxAvg(Expr expr) throws VerdictException {
        return TypeCasting.toDouble(approxAgg(FuncExpr.avg(expr)).collect().get(0).get(0));
    }

    public long approxCountDistinct(Expr expr) throws VerdictException {
        return TypeCasting.toLong(approxAgg(FuncExpr.countDistinct(expr)).collect().get(0).get(0));
    }

    public long approxCountDistinct(String expr) throws VerdictException {
        return TypeCasting.toLong(approxAgg(FuncExpr.countDistinct(Expr.from(vc, expr))).collect().get(0).get(0));
    }

    /*
     * order by and limit
     */

    public ExactRelation orderby(String orderby) {
        String[] tokens = orderby.split(",");
        List<OrderByExpr> cols = new ArrayList<OrderByExpr>();
        for (String t : tokens) {
            cols.add(OrderByExpr.from(vc, t));
        }
        return new OrderedRelation(vc, this, cols);
    }

    public ExactRelation limit(long limit) {
        return new LimitedRelation(vc, this, limit);
    }

    public ExactRelation limit(String limit) {
        return limit(Integer.valueOf(limit));
    }

    /*
     * Joins
     */

    public JoinedRelation join(ExactRelation r, List<Pair<Expr, Expr>> joinColumns) {
        return JoinedRelation.from(vc, this, r, joinColumns);
    }

    public JoinedRelation join(ExactRelation r, Cond cond) throws VerdictException {
        return JoinedRelation.from(vc, this, r, cond);
    }

    public JoinedRelation join(ExactRelation r, String cond) throws VerdictException {
        return join(r, Cond.from(vc, cond));
    }

    public JoinedRelation join(ExactRelation r) throws VerdictException {
        return join(r, (Cond) null);
    }	

    public JoinedRelation leftjoin(ExactRelation r, List<Pair<Expr, Expr>> joinColumns) {
        JoinedRelation j = join(r, joinColumns);
        j.setJoinType("LEFT");
        return j;
    }

    public JoinedRelation leftjoin(ExactRelation r, Cond cond) throws VerdictException {
        JoinedRelation j = join(r, cond);
        j.setJoinType("LEFT");
        return j;
    }

    public JoinedRelation leftjoin(ExactRelation r, String cond) throws VerdictException {
        JoinedRelation j = join(r, cond);
        j.setJoinType("LEFT");
        return j;
    }

    public JoinedRelation leftjoin(ExactRelation r) throws VerdictException {
        JoinedRelation j = join(r);
        j.setJoinType("LEFT");
        return j;
    }

    /**
     * Transforms to an ApproxRelation.
     * 
     * The main function of this method is to find a best set of samples for approximate computations of a given query.
     * {@link AggregatedRelation#approx()} is mostly responsible for this.
     */
    public abstract ApproxRelation approx() throws VerdictException;

    protected abstract ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace);

    /**
     * Finds sets of samples that could be used for the table sources in a transformed approximate relation.
     * Called on ProjectedRelation or AggregatedRelation, returns an empty set.
     * Called on FilteredRelation, returns the result of its source.
     * Called on JoinedRelation, combine the results of its two sources.
     * Called on SingleRelation, finds a proper list of samples.
     * Note that the return value's key (i.e., Set<ApproxSingleRelation>) holds a set of samples that point to all
     * different relations. In other words, if this sql includes two tables, then the size of the set will be two, and
     * the elements of the set will be the sample tables for those two tables. Multiple of such sets serve as candidates.
     * @param functions
     * @return A map from a candidate to [cost, sampling prob].
     */
    protected List<SampleGroup> findSample(Expr elem) {
        return new ArrayList<SampleGroup>();
    }

    /**
     * Finds 'n' best ApproxRelation instances that can produce similar answers for aggregate expressions.
     * This method is expected to be called by AggregatedRelation initially and propagate recursively to
     * descendants to properly build ApproxRelation instances that reflect the structure of the current
     * ExactRelation instance.
     * 
     * One important characteristic is that the returned ApproxRelation instances must be able to used by
     * an outer AggregatedRelation for producing properly adjusted answers. This means that the ApproxRelation
     * returned by this method must be associated with sampling probability and include two extra meta columns,
     * i.e., __vpart and __vprob. One possible exception to this rule is that the source relation is an AggregatedRelataion
     * or an ProjectedRelation whose source is an AggregatedRelataion. This is because, in the aggregation
     * process, __vprob becomes meaningless. In this case, the aggregated source relation must attach
     * __vprob as an constant, and let the information known to outer relations when {@link ApproxRelation#samplingProbabilityExprsFor(FuncExpr)}
     * is called.
     * 
     * If a source relation is an groupby aggregated relation, the aggregated relation's source must be
     * a universe sample.
     * @param elem
     * @param n
     * @return
     * @throws VerdictException
     */
    protected abstract List<ApproxRelation> nBestSamples(Expr elem, int n) throws VerdictException;

    /**
     * Note that {@link ExactRelation#findSample(Expr) findSample} method obtains candidate sample sets for every
     * (aggregate) expression. This function checks if some of them can be computed using the same sample set. If doing
     * so can save time, we compute them using the same sample set.
     * @param candidates_list
     * @return
     */
    protected SamplePlan consolidate(
            List<List<SampleGroup>> candidates_list) {
        SamplePlans plans = new SamplePlans();

        // create candidate plans
        for (List<SampleGroup> groups : candidates_list) {
            plans.consolidateNewExpr(groups);
        }

        double relative_cost_ratio = vc.getConf().getRelativeTargetCost();
        SamplePlan best = plans.bestPlan(relative_cost_ratio);
        return best;
    }

    /*
     * Helpers
     */

    /**
     * 
     * @param relation Starts to collect from this relation
     * @return All found groupby expressions and the first relation that is not a GroupedRelation.
     */
    protected static Pair<List<Expr>, ExactRelation> allPrecedingGroupbys(ExactRelation r) {
        List<Expr> groupbys = new ArrayList<Expr>();
        ExactRelation t = r;
        while (true) {
            if (t instanceof GroupedRelation) {
                groupbys.addAll(((GroupedRelation) t).groupby);
                t = ((GroupedRelation) t).getSource();
            } else {
                break;
            }
        }
        return Pair.of(groupbys, t);
    }

    /**
     * Collects all the filters in the antecedents of the parameter relation.
     * @param r
     * @return
     */
    protected Pair<Optional<Cond>, ExactRelation> allPrecedingFilters(ExactRelation r) {
        Optional<Cond> c = Optional.absent();
        ExactRelation t = r;
        while (true) {
            if (t instanceof FilteredRelation) {
                if (c.isPresent()) {
                    c = Optional.of((Cond) AndCond.from(c.get(), ((FilteredRelation) t).getFilter()));
                } else {
                    c = Optional.of(((FilteredRelation) t).getFilter());
                }
                t = ((FilteredRelation) t).getSource();
            } else {
                break;
            }
        }
        return Pair.of(c, t);
    }

    protected String sourceExpr(ExactRelation source) {
        if (source instanceof SingleRelation) {
            SingleRelation asource = (SingleRelation) source;
            TableUniqueName tableName = asource.getTableName();
            String alias = asource.getAlias();
            return String.format("%s AS %s", tableName, alias);
        } else if (source instanceof JoinedRelation) {
            return ((JoinedRelation) source).joinClause();
        } else {
            String alias = source.getAlias();
            if (alias == null) {
                alias = Relation.genTableAlias();
            }
            return String.format("(%s) AS %s", source.toSql(), alias);
        }
    }

    //	/**
    //	 * This function tracks select list elements whose answers could be approximate when run on a sample.
    //	 * @return
    //	 */
    //	public List<SelectElem> selectElemsWithAggregateSource() {
    //		return new ArrayList<SelectElem>();
    //	}

    /**
     * Used for subsampling-based error estimation. Return the partition column of this instance.
     * 
     * How a partition column is determined:
     * <ul>
     * <li>SingleRelation: partition column must exist if a sample table. If not, {@link ExactRelation#partitionColumn()} returns null. </li>
     * <li>JoinedRelation: find a first-found sample table</li>
     * <li>ProjectedRelation: the partition column of a source must be preserved.</li>
     * <li>AggregatedRelation: the partition column of a source must be preserved by inserting an extra groupby column.</li>
     * <li>GroupedRelation: the partition column of a source must be preserved by inserting an extra groupby column.</li>
     * </ul>
     * 
     * @return
     */
    public abstract ColNameExpr partitionColumn();

    @Deprecated
    public abstract List<ColNameExpr> accumulateSamplingProbColumns();

    @Override
    public String toString() {
        return toStringWithIndent("");
    }

    protected abstract String toStringWithIndent(String indent);

}


class RelationGen extends VerdictSQLBaseVisitor<ExactRelation> {

    private VerdictContext vc;

    public RelationGen(VerdictContext vc) {
        this.vc = vc;
    }

    @Override
    public ExactRelation visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
        ExactRelation r = visit(ctx.query_expression());

        if (ctx.order_by_clause() != null) {
            List<OrderByExpr> orderby = new ArrayList<OrderByExpr>();
            for (Order_by_expressionContext o : ctx.order_by_clause().order_by_expression()) {
                orderby.add(new OrderByExpr(vc, Expr.from(vc, o.expression()),
                        (o.DESC() != null)? "DESC" : "ASC"));
            }
            r = new OrderedRelation(vc, r, orderby);
        }

        if (ctx.limit_clause() != null) {
            r = r.limit(ctx.limit_clause().number().getText());
        }

        return r;
    }

    @Override
    public ExactRelation visitQuery_specification(VerdictSQLParser.Query_specificationContext ctx) {
        // parse the where clause
        Cond where = null;
        if (ctx.WHERE() != null) {
            where = Cond.from(vc, ctx.where);
        }

        // parse the from clause
        // if a subquery is found; another instance of this class will be created and be used.
        // we convert all the table sources into ExactRelation instances. Those ExactRelation instances will be joined
        // either using the join condition explicitly stated using the INNER JOIN ON statements or using the conditions
        // in the where clause.
        ExactRelation r = null;
        List<String> joinedTableName = new ArrayList<String>();

        for (Table_sourceContext s : ctx.table_source()) {
            TableSourceExtractor e = new TableSourceExtractor();
            ExactRelation r1 = e.visit(s);
            if (r == null) r = r1;
            else {
                JoinedRelation r2 = new JoinedRelation(vc, r, r1, null);
                Cond j = null;

                // search for join conditions
                if (r1 instanceof SingleRelation && where != null) {
                    String n = ((SingleRelation) r1).getTableName().getTableName();
                    j = where.searchForJoinCondition(joinedTableName, n);
                    if (j == null && r1.getAlias() != null) {
                        j = where.searchForJoinCondition(joinedTableName, r1.getAlias());
                    }
                } else if (r2.getAlias() != null && where != null) {
                    j = where.searchForJoinCondition(joinedTableName, r2.getAlias());
                }

                if (j != null) {
                    try {
                        r2.setJoinCond(j);
                    } catch (VerdictException e1) {
                        VerdictLogger.error(StackTraceReader.stackTrace2String(e1));
                    }
                    where = where.remove(j);
                }

                r = r2;
            }

            // add both table names and the alias to joined table names
            if (r1 instanceof SingleRelation) {
                joinedTableName.add(((SingleRelation) r1).getTableName().getTableName());
            }
            if (r1.getAlias() != null) {
                joinedTableName.add(r1.getAlias());
            }
        }

        if (where != null) {
            r = new FilteredRelation(vc, r, where);
        }

        // parse select list first
        SelectListExtractor select = new SelectListExtractor();
        Triple<List<SelectElem>, List<SelectElem>, List<SelectElem>> elems = select.visit(ctx.select_list());
        List<SelectElem> nonaggs = elems.getLeft();
        List<SelectElem> aggs = elems.getMiddle();
        List<SelectElem> bothInOrder = elems.getRight();

        // obtains groupby expressions
        // groupby may include an aliased select elem. In that case, we use the original select elem expr.
        if (ctx.GROUP() != null) {
            List<Expr> groupby = new ArrayList<Expr>();
            for (Group_by_itemContext g : ctx.group_by_item()) {
                Expr gexpr = Expr.from(vc, g.expression());
                boolean aliasFound = false;

                // search in alises
                for (SelectElem s : bothInOrder) {
                    if (s.aliasPresent() && gexpr.toStringWithoutQuote().equals(s.getAlias())) {
                        groupby.add(s.getExpr());
                        aliasFound = true;
                        break;
                    }
                }

                if (!aliasFound) {
                    groupby.add(gexpr);
                }
            }
            r = new GroupedRelation(vc, r, groupby);
        }

        if (aggs.size() > 0) {		// if there are aggregate functions
            List<Expr> aggExprs = new ArrayList<Expr>();
            for (SelectElem se : aggs) {
                aggExprs.add(se.getExpr());
            }
            r = new AggregatedRelation(vc, r, aggExprs);
            //			r.setAliasName(Relation.genTableAlias());

            // we put another layer on top of AggregatedRelation for
            // 1. the case where the select list does not include all of groupby and aggregate expressions.
            // 2. the select elems are not in the order of groupby and aggregations.
            List<SelectElem> prj = new ArrayList<SelectElem>();
            for (SelectElem e : bothInOrder) {
                prj.add(e);
                //				if (aggs.contains(e)) {
                //					// based on the assumption that agg expressions are always aliased.
                //					// we define an alias if it doesn't exists.
                //					prj.add(new SelectElem(Expr.from(e.getAlias()), e.getAlias()));
                //				} else {
                //					if (e.aliasPresent()) {
                //						prj.add(new SelectElem(e.getExpr(), e.getAlias()));
                //					} else {
                //						prj.add(e);
                //					}	
                //				}
            }
            r = new ProjectedRelation(vc, r, prj);
        } else {
            r = new ProjectedRelation(vc, r, bothInOrder);
        }

        return r;
    }

    // Returs a triple of
    // 1. non-aggregate select list elements
    // 2. aggregate select list elements.
    // 3. both of them in order.
    class SelectListExtractor extends VerdictSQLBaseVisitor<Triple<List<SelectElem>, List<SelectElem>, List<SelectElem>>> {
        @Override public Triple<List<SelectElem>, List<SelectElem>, List<SelectElem>> visitSelect_list(VerdictSQLParser.Select_listContext ctx) {
            List<SelectElem> nonagg = new ArrayList<SelectElem>();
            List<SelectElem> agg = new ArrayList<SelectElem>();
            List<SelectElem> both = new ArrayList<SelectElem>();
            for (Select_list_elemContext a : ctx.select_list_elem()) {
                SelectElem e = SelectElem.from(vc, a);
                if (e.isagg()) {
                    agg.add(e);
                } else {
                    nonagg.add(e);
                }
                both.add(e);
            }
            return Triple.of(nonagg, agg, both);
        }
    }

    // The tableSource returned from this class is supported to include all necessary join conditions; thus, we do not
    // need to search for their join conditions in the where clause.
    class TableSourceExtractor extends VerdictSQLBaseVisitor<ExactRelation> {
        public List<ExactRelation> relations = new ArrayList<ExactRelation>();

        private Cond joinCond = null;

        @Override
        public ExactRelation visitTable_source_item_joined(VerdictSQLParser.Table_source_item_joinedContext ctx) {
            ExactRelation r = visit(ctx.table_source_item());
            for (Join_partContext j : ctx.join_part()) {
                ExactRelation r2 = visit(j);
                r = new JoinedRelation(vc, r, r2, null);
                if (joinCond != null) {
                    try {
                        ((JoinedRelation) r).setJoinCond(joinCond);
                    } catch (VerdictException e) {
                        VerdictLogger.error(StackTraceReader.stackTrace2String(e));
                    }
                    joinCond = null;
                }
            }
            return r;
        }

        @Override
        public ExactRelation visitHinted_table_name_item(VerdictSQLParser.Hinted_table_name_itemContext ctx) {
            String tableName = ctx.table_name_with_hint().table_name().getText();
            ExactRelation r = SingleRelation.from(vc, tableName);
            if (ctx.as_table_alias() != null) {
                r.setAlias(ctx.as_table_alias().table_alias().getText());
            }
            return r;
        }

        @Override
        public ExactRelation visitDerived_table_source_item(VerdictSQLParser.Derived_table_source_itemContext ctx) {
            RelationGen gen = new RelationGen(vc);
            ExactRelation r = gen.visit(ctx.derived_table().subquery().select_statement());
            if (ctx.as_table_alias() != null) {
                r.setAlias(ctx.as_table_alias().table_alias().getText());
            }
            return r;
        }

        @Override
        public ExactRelation visitJoin_part(VerdictSQLParser.Join_partContext ctx) {
            if (ctx.INNER() != null) {
                TableSourceExtractor ext = new TableSourceExtractor();
                ExactRelation r = ext.visit(ctx.table_source());
                joinCond = Cond.from(vc, ctx.search_condition());
                return r;
            } else {
                VerdictLogger.error(this, "Unsupported join condition: " + ctx.getText());
                return null;
            }
        }
    }

    //	protected String getOriginalText(ParserRuleContext ctx) {
    //		int a = ctx.start.getStartIndex();
    //	    int b = ctx.stop.getStopIndex();
    //	    Interval interval = new Interval(a,b);
    //	    return CharStreams.fromString(sql).getText(interval);
    //	}
}

