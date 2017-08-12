package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import edu.umich.verdict.relation.condition.CompCond;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.condition.CondModifier;
import edu.umich.verdict.relation.condition.InCond;
import edu.umich.verdict.relation.condition.IsCond;
import edu.umich.verdict.relation.condition.LikeCond;
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
        
        // add groupby list
        if (this instanceof GroupedRelation) {
            List<Expr> groupby = ((GroupedRelation) this).getGroupby();
            for (Expr group : groupby) {
                se.add(new SelectElem(vc, group));
            }
        }
        
        for (Object e : elems) {
            if (e instanceof SelectElem) {
                se.add((SelectElem) e);
            } else {
                se.add(SelectElem.from(vc, e.toString()));
            }
        } 

//        List<Expr> exprs = new ArrayList<Expr>();
//        for (SelectElem elem : se) {
//            exprs.add(elem.getExpr());
//        }

        ExactRelation r = new AggregatedRelation(vc, this, se);
//        r = new ProjectedRelation(vc, this, se);
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
    protected SamplePlans consolidate(List<List<SampleGroup>> candidates_list) {
        SamplePlans plans = new SamplePlans();
        // create candidate plans
        for (List<SampleGroup> groups : candidates_list) {
            plans.consolidateNewExpr(groups);
        }
        return plans;
//        double relative_cost_ratio = vc.getConf().getRelativeTargetCost();
//        SamplePlan best = plans.bestPlan(relative_cost_ratio);
//        return best;
    }
    
    protected SamplePlan chooseBestPlan(SamplePlans plans) {
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
     * <li>SingleRelation: partition column must exist if it's a sample table. If not, {@link ExactRelation#partitionColumn()} returns null. </li>
     * <li>JoinedRelation: find a first-found sample table</li>
     * <li>ProjectedRelation: the partition column of a source must be preserved.</li>
     * <li>AggregatedRelation: the partition column of a source must be preserved by inserting an extra groupby column.</li>
     * <li>GroupedRelation: the partition column of a source must be preserved by inserting an extra groupby column.</li>
     * </ul>
     * 
     * @return
     */
    public abstract ColNameExpr partitionColumn();
    
//    public abstract Expr distinctCountPartitionColumn();

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
    
    // we remember what base tables have been joined (or appeared). this information is used for
    // replacing original table names with aliases in join conditions, and other column name expressions.
    // also, we use this field to attach effective table names to column names.
    private Map<TableUniqueName, Pair<String, Set<String>>> tableAliasAndColNames = new HashMap<TableUniqueName, Pair<String, Set<String>>>();

    @Override
    public ExactRelation visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
        ExactRelation r = visit(ctx.query_expression());
        
        // table name replacer with aliases
//        TableSourceResolver resolver = new TableSourceResolver(vc, tableAliasAndColNames);

        if (ctx.order_by_clause() != null) {
            List<OrderByExpr> orderby = new ArrayList<OrderByExpr>();
            for (Order_by_expressionContext o : ctx.order_by_clause().order_by_expression()) {
                OrderByExpr expr = new OrderByExpr(vc, Expr.from(vc, o.expression()),
                                                   (o.DESC() != null)? "DESC" : "ASC");
//                orderby.add((OrderByExpr) resolver.visit(expr));
                orderby.add(expr);
            }
            r = new OrderedRelation(vc, r, orderby);
        }

        if (ctx.limit_clause() != null) {
            r = r.limit(ctx.limit_clause().number().getText());
        }

        return r;
    }
    
    class TableSourceResolver extends TableNameReplacerInExpr {
        
        private Map<TableUniqueName, Pair<String, Set<String>>> tabAliasColumns;
        
        public TableSourceResolver(VerdictContext vc, Map<TableUniqueName, Pair<String, Set<String>>> tabAliasColumns) {
            super(vc, null);
            this.tabAliasColumns = tabAliasColumns;
        }
        
        @Override
        protected Expr replaceColNameExpr(ColNameExpr expr) {
            if (expr.getTab() != null) {
                if (expr.getSchema() != null) {
                    // this must be a full table name (not an alias)
                    TableUniqueName t = new TableUniqueName(expr.getSchema(), expr.getTab());
                    if (tabAliasColumns.containsKey(t)) {
                        // this is an expected case.
                        return new ColNameExpr(vc, expr.getCol(), tabAliasColumns.get(t).getKey());
                    }
                } else {
                    // table could be an alias or a base table name without schema name
                    // first, check for alias
                    TableUniqueName a = new TableUniqueName(null, expr.getTab());
                    if (tabAliasColumns.containsKey(a)) {
                        return new ColNameExpr(vc, expr.getCol(), tabAliasColumns.get(a).getKey());
                    }
                    // second, check for table name
                    TableUniqueName t = TableUniqueName.uname(vc, expr.getTab());
                    if (tabAliasColumns.containsKey(t)) {
                        return new ColNameExpr(vc, expr.getCol(), tabAliasColumns.get(t).getKey());
                    }
                }
            } else {
                // need to find the table that has this column.
                String col = expr.getCol();
                for (Map.Entry<TableUniqueName, Pair<String, Set<String>>> e : tabAliasColumns.entrySet()) {
                    Pair<String, Set<String>> aliasCols = e.getValue();
                    if (aliasCols.getValue().contains(col)) {
                        return new ColNameExpr(vc, col, aliasCols.getKey());
                    }
                }
            }
            
            VerdictLogger.error(this, String.format("The specified column, %s, is not found in the tables in the from clause.", expr.toString()));
            return expr;
        }
    }
    
    class ColNameResolver extends CondModifier {
        
        private Map<TableUniqueName, Pair<String, Set<String>>> baseTables;
        
        TableSourceResolver resolver;
        
        public ColNameResolver(Map<TableUniqueName, Pair<String, Set<String>>> baseTables) {
            this.baseTables = baseTables;
            this.resolver = new TableSourceResolver(vc, this.baseTables);
        }        
        
        @Override
        public Cond call(Cond cond) {
            if (cond instanceof CompCond) {
                Expr le = ((CompCond) cond).getLeft();
                Expr re = ((CompCond) cond).getRight();
                
                le = resolver.visit(le);
                re = resolver.visit(re);
                
                return new CompCond(le, ((CompCond) cond).getOp(), re);
            } else if (cond instanceof LikeCond) {
                Expr le = ((LikeCond) cond).getLeft();
                Expr re = ((LikeCond) cond).getRight();
                
                le = resolver.visit(le);
                re = resolver.visit(re);
                
                return new LikeCond(le, re, ((LikeCond) cond).isNot());
            } else if (cond instanceof InCond) {
                Expr le = ((InCond) cond).getLeft();
                le = resolver.visit(le);
                return new InCond(le, ((InCond) cond).isNot(), ((InCond) cond).getExpressionList());
            } else if (cond instanceof IsCond) {
                Expr le = ((IsCond) cond).getLeft();
                le = resolver.visit(le);
                return new IsCond(le, ((IsCond) cond).getRight());
            } else {
                return cond;
            }
        }
        
    }

    /**
     * Parses a depth-one select statement. If there exist subqueries, this function will be called recursively.
     */
    @Override
    public ExactRelation visitQuery_specification(VerdictSQLParser.Query_specificationContext ctx) {
        // 1. extract all tables objects for creating joined table sources later.
        //    the complete INNER JOIN expressions is converted to a single ExactRelation object.
        // 2. extract all base table names for column name resolution.
        //    a. if a user defines an alias for a table, we expect he is using the alias for in column names in place of
        //       of the original table names. If no table name is specified, we find relevant tables using the base table
        //       names and insert aliases for those column names..
        //    b. if a user doesn't define an alias, we generate a random alias, performs the same process using the
        //       auto-generated alias.
        //    c. if a user defines an alias for a derived table (he must do so), we extract the column names for the derived table.
        List<ExactRelation> tableSources = new ArrayList<ExactRelation>();      // assume that only the first entry can be JoinedRelation
        for (Table_sourceContext s : ctx.table_source()) {
            TableSourceExtractor e = new TableSourceExtractor();
            ExactRelation r1 = e.visit(s);
            tableSources.add(r1);
            if (r1 instanceof SingleRelation) {
                TableUniqueName tableName = ((SingleRelation) r1).getTableName();
                String alias = r1.getAlias();
                Set<String> colNames = vc.getMeta().getColumns(tableName);
                tableAliasAndColNames.put(tableName, Pair.of(alias, colNames));
            } else if (r1 instanceof ProjectedRelation) {
                TableUniqueName tableName = new TableUniqueName(null, r1.getAlias());       // just use alias name
                List<SelectElem> elems = ((ProjectedRelation) r1).getSelectElems();
                Set<String> colNames = new HashSet<String>();
                for (SelectElem elem : elems) {
                    if (elem.aliasPresent()) {
                        colNames.add(elem.getAlias());
                    } else {
                        colNames.add(elem.getExpr().toSql());
                    }
                }
                tableAliasAndColNames.put(tableName, Pair.of(r1.getAlias(), colNames));
            } else if (r1 instanceof AggregatedRelation) {
                TableUniqueName tableName = new TableUniqueName(null, r1.getAlias());       // just use alias name
                List<SelectElem> elems = ((AggregatedRelation) r1).getElemList();
                Set<String> colNames = new HashSet<String>();
                for (SelectElem elem : elems) {
                    if (elem.aliasPresent()) {
                        colNames.add(elem.getAlias());
                    } else {
                        colNames.add(elem.getExpr().toSql());
                    }
                }
                tableAliasAndColNames.put(tableName, Pair.of(r1.getAlias(), colNames));
            }
        }
        
        // parse the where clause; we also replace all base table names with their alias names.
        Cond where = null;
        if (ctx.WHERE() != null) {
            where = Cond.from(vc, ctx.where);
            ColNameResolver resolver = new ColNameResolver(tableAliasAndColNames);
            where = resolver.visit(where);
            // at this point, all table names in the where clause are all aliased.
        }
        
        // In the where clause, we search for the conditions for inner joins.
        // A filtering predicate (or Cond instance) is recognized as a join condition
        // if it's an instance of CompCond class and the left and the right sides of the condition include column names
        // of different tables.
        // This means that some general join expressions such as "col1 is null and col2 is null" are not correctly recognized.
        // Support such general join expressions is a TODO item.
        ExactRelation joinedTabeSource = null;
        while (where != null && tableSources.size() > 1) {
            Pair<Cond, Pair<ExactRelation, ExactRelation>> joinCondAndTabName = where.searchForJoinCondition(tableSources);
            if (joinCondAndTabName == null) {
                break;
            }
            
            // create a joined table source
            Cond joinCond = joinCondAndTabName.getKey();
            Pair<ExactRelation, ExactRelation> pairsToJoin = joinCondAndTabName.getValue();
            List<ExactRelation> newTableSources = new ArrayList<ExactRelation>();
            ExactRelation joined = JoinedRelation.from(vc, pairsToJoin.getLeft(), pairsToJoin.getRight(), joinCond);
            
            newTableSources.add(joined);
            for (ExactRelation t : tableSources) {
                if (t != pairsToJoin.getLeft() && t != pairsToJoin.getRight()) {
                    newTableSources.add(t);
                }
            }
            tableSources = newTableSources;
            where = where.remove(joinCond);
        }
        
        // if there is any table sources left, they should be cross-joined.
        for (ExactRelation r : tableSources) {
            if (joinedTabeSource == null) {
                joinedTabeSource = r;
            } else {
                joinedTabeSource = new JoinedRelation(vc, joinedTabeSource, r, null);
            }
        }

        if (where != null) {
            joinedTabeSource = new FilteredRelation(vc, joinedTabeSource, where);
        }

        // parse select list
        SelectListExtractor select = new SelectListExtractor();
        Triple<List<SelectElem>, List<SelectElem>, List<SelectElem>> elems = select.visit(ctx.select_list());
        List<SelectElem> nonaggs = elems.getLeft();
        List<SelectElem> aggs = elems.getMiddle();
        List<SelectElem> bothInOrder = elems.getRight();
        
        // replace all base tables with their aliases
        TableSourceResolver resolver = new TableSourceResolver(vc, tableAliasAndColNames);
        nonaggs = replaceTableNamesWithAliasesIn(nonaggs, resolver);
        aggs = replaceTableNamesWithAliasesIn(aggs, resolver);
        bothInOrder = replaceTableNamesWithAliasesIn(bothInOrder, resolver);
        
        if (aggs.size() == 0) {
            // simple projection
            joinedTabeSource = new ProjectedRelation(vc, joinedTabeSource, bothInOrder);
        } else {
            // aggregate relation
            
            // step 1: obtains groupby expressions
            // groupby expressions must be fully resolved from the table sources (without referring to the select list)
            // resolving groupby names from alises is currently disabled.
            // if the groupby expression includes base table names, we replace them with their aliases.
            if (ctx.GROUP() != null) {
                List<Expr> groupby = new ArrayList<Expr>();
                for (Group_by_itemContext g : ctx.group_by_item()) {
                    Expr gexpr = resolver.visit(Expr.from(vc, g.expression()));
                    boolean aliasFound = false;
    //
//                    // search in alises
//                    for (SelectElem s : bothInOrder) {
//                        if (s.aliasPresent() && gexpr.toStringWithoutQuote().equals(s.getAlias())) {
//                            groupby.add(s.getExpr());
//                            aliasFound = true;
//                            break;
//                        }
//                    }

                    if (!aliasFound) {
                        groupby.add(gexpr);
                    }
                }
                joinedTabeSource = new GroupedRelation(vc, joinedTabeSource, groupby);
            }
            
            joinedTabeSource = new AggregatedRelation(vc, joinedTabeSource, bothInOrder);
        }

        return joinedTabeSource;
    }
    
    private List<SelectElem> replaceTableNamesWithAliasesIn(List<SelectElem> elems, TableSourceResolver resolver) {
        List<SelectElem> substituted = new ArrayList<SelectElem>();
        for (SelectElem elem : elems) {
            Expr replaced = resolver.visit(elem.getExpr());
            substituted.add(new SelectElem(elem.getVerdictContext(), replaced, elem.getAlias()));
        }
        return substituted;
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
            TableUniqueName tabName = ((SingleRelation) r).getTableName();
            Set<String> colNames = vc.getMeta().getColumns(tabName);
            tableAliasAndColNames.put(tabName, Pair.of(r.getAlias(), colNames));
            return r;
        }

        @Override
        public ExactRelation visitDerived_table_source_item(VerdictSQLParser.Derived_table_source_itemContext ctx) {
            RelationGen gen = new RelationGen(vc);
            ExactRelation r = gen.visit(ctx.derived_table().subquery().select_statement());
            if (ctx.as_table_alias() != null) {
                r.setAlias(ctx.as_table_alias().table_alias().getText());
            }
            
            Set<String> colNames = new HashSet<String>();
            if (r instanceof AggregatedRelation) {
                List<SelectElem> elems = ((AggregatedRelation) r).getElemList();
                for (SelectElem elem : elems) {
                    if (elem.aliasPresent()) {
                        colNames.add(elem.getAlias());
                    } else {
                        colNames.add(elem.getExpr().toSql());
                    }
                }
            }
            TableUniqueName tabName = new TableUniqueName(null, r.getAlias());
            tableAliasAndColNames.put(tabName, Pair.of(r.getAlias(), colNames));
            return r;
        }

        @Override
        public ExactRelation visitJoin_part(VerdictSQLParser.Join_partContext ctx) {
            if (ctx.INNER() != null) {
                TableSourceExtractor ext = new TableSourceExtractor();
                ExactRelation r = ext.visit(ctx.table_source());
                Cond cond = Cond.from(vc, ctx.search_condition());
                ColNameResolver resolver = new ColNameResolver(tableAliasAndColNames);
                Cond resolved = resolver.visit(cond);
                
                if (resolved instanceof CompCond) {
                    CompCond comp = (CompCond) resolved;
                    Expr right = comp.getRight();
                    if (right instanceof ColNameExpr) {
                        if (((ColNameExpr) right).getCol() != r.getAlias()) {
                            resolved = new CompCond(comp.getRight(), comp.getOp(), comp.getLeft());
                        }
                    }
                }
                
                joinCond = resolved;
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

