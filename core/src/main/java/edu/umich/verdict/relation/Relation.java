/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.relation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.commons.lang3.tuple.Pair;
//import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.exceptions.VerdictUnexpectedMethodCall;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.parser.VerdictSQLParser.ExpressionContext;
import edu.umich.verdict.parser.VerdictSQLParser.Join_partContext;
import edu.umich.verdict.parser.VerdictSQLParser.Search_conditionContext;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.ExprModifier;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.OrderByExpr;
import edu.umich.verdict.relation.expr.OverClause;
import edu.umich.verdict.relation.expr.SelectElem;
import edu.umich.verdict.relation.expr.SubqueryExpr;
import edu.umich.verdict.util.ResultSetConversion;
import edu.umich.verdict.util.StackTraceReader;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.TypeCasting;
import edu.umich.verdict.util.VerdictLogger;

/**
 * Both {@link ExactRelation} and {@link ApproxRelation} must extends this
 * class.
 * 
 * @author Yongjoo Park
 *
 */
public abstract class Relation {

    protected VerdictContext vc;

    protected boolean subquery;

    protected boolean approximate;

    protected String alias;

    protected static final String UNKNOWN_TABLE_FOR_WINDOW_FUNC = "???VerdictDB_Table???";

    /**
     * uniform: uniform random sample arbitrary: sampling probabilities for tuples
     * may be all different. (there's no guarantee on uniformness). nosample: the
     * original table itself. stratified: stratified on a certain column. It is
     * guaranteed that we do not miss any group. universe: hashed on a certain
     * column. It is guaranteed that the tuples with the same attribute values in
     * the column are sampled together.
     */
    public final static Set<String> availableJoinTypes = Sets.newHashSet("uniform", "universe", "stratified",
            "nosample", "arbitrary");

    public Relation(VerdictContext vc) {
        this.vc = vc;
        this.subquery = false;
        this.approximate = false;
        this.alias = genTableAlias();
    }

    public VerdictContext getVerdictContext() {
        return vc;
    }

    public boolean isSubquery() {
        return subquery;
    }

    public void setSubquery(boolean a) {
        this.subquery = a;
    }

    public boolean isApproximate() {
        return approximate;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String a) {
        alias = a.toLowerCase();
    }

    /**
     * Expression that would appear in sql statement. SingleSourceRelation: table
     * name JoinedSourceRelation: join expression FilteredRelation: select * where
     * condition from sourceExpr() AggregatedRelation: select groupby, agg where
     * condition from sourceExpr()
     * 
     * @return
     * @throws VerdictUnexpectedMethodCall
     */
    public abstract String toSql();

    /*
     * Aggregation
     */

    public long countValue() throws VerdictException {
        return TypeCasting.toLong(count().collect().get(0).get(0));
    }

    public double sumValue(String expr) throws VerdictException {
        return TypeCasting.toDouble(sum(expr).collect().get(0).get(0));
    }

    public double avgValue(String expr) throws VerdictException {
        return TypeCasting.toDouble(avg(expr).collect().get(0).get(0));
    }

    public long countDistinctValue(String expr) throws VerdictException {
        return TypeCasting.toLong(countDistinct(expr).collect().get(0).get(0));
    }

    public abstract Relation count() throws VerdictException;

    public abstract Relation sum(String expr) throws VerdictException;

    public abstract Relation avg(String expr) throws VerdictException;

    public abstract Relation countDistinct(String expr) throws VerdictException;

    /*
     * Collect results
     */

    public ResultSet collectResultSet() throws VerdictException {
        String sql = toSql();
        VerdictLogger.debug(this, "A query to db: " + sql);
//        VerdictLogger.debug(this, "A query to db:");
//        VerdictLogger.debugPretty(this, Relation.prettyfySql(vc, sql), " ");
        ResultSet rs = vc.getDbms().executeJdbcQuery(sql);
        return rs;
    }

//    public DataFrame collectDataFrame() throws VerdictException {
//        String sql = toSql();
//        VerdictLogger.debug(this, "A query to db: " + sql);
////        VerdictLogger.debug(this, "A query to db:");
////        VerdictLogger.debugPretty(this, Relation.prettyfySql(vc, sql), " ");
//        DataFrame df = vc.getDbms().executeSparkQuery(sql);
//        return df;
//    }

    public Dataset<Row> collectDataset() throws VerdictException {
        String sql = toSql();
        VerdictLogger.debug(this, "A query to db: " + sql);
//        VerdictLogger.debug(this, "A query to db:");
//        VerdictLogger.debugPretty(this, Relation.prettyfySql(vc, sql), " ");
        Dataset<Row> ds = vc.getDbms().executeSpark2Query(sql);
        return ds;
    }

    public List<List<Object>> collect() throws VerdictException {
        List<List<Object>> result = new ArrayList<List<Object>>();
        if (vc.getDbms().isJDBC()) {
            ResultSet rs = collectResultSet();
            try {
                int colCount = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    List<Object> row = new ArrayList<Object>();
                    for (int i = 1; i <= colCount; i++) {
                        row.add(rs.getObject(i));
                    }
                    result.add(row);
                }
                rs.close();
            } catch (SQLException e) {
                throw new VerdictException(e);
            }
//        } else if (vc.getDbms().isSpark()) {
//            DataFrame df = collectDataFrame();
//            List<Row> rows = df.collectAsList();
//            for (Row r : rows) {
//                int size = r.size();
//                List<Object> row = new ArrayList<Object>();
//                for (int i = 0; i < size; i++) {
//                    row.add(r.get(i));
//                }
//                result.add(row);
//            }
        } else if (vc.getDbms().isSpark2()) {
            Dataset<Row> ds = collectDataset();
            List<Row> rows = ds.collectAsList();
            for (Row r : rows) {
                int size = r.size();
                List<Object> row = new ArrayList<Object>();
                for (int i = 0; i < size; i++) {
                    row.add(r.get(i));
                }
                result.add(row);
            }
        }
        return result;
    }

    public String collectAsString() {
        try {
            return ResultSetConversion.resultSetToString(collectResultSet());
        } catch (VerdictException e) {
            return StackTraceReader.stackTrace2String(e);
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }

    /*
     * Helpers
     */

    protected List<Expr> exprsInSelectElems(List<SelectElem> elems) {
        List<Expr> exprs = new ArrayList<Expr>();
        for (SelectElem e : elems) {
            exprs.add(e.getExpr());
        }
        return exprs;
    }

    public static String prettyfySql(VerdictContext vc, String sql) {
        VerdictSQLParser p = StringManipulations.parserOf(sql);
        PrettyPrintVisitor r = new PrettyPrintVisitor(vc, sql);
        return r.visit(p.verdict_statement());
    }

    private static int tab_alias_no = 1;

    public static String genTableAlias() {
        String n = String.format("vt%d", tab_alias_no);
        tab_alias_no++;
        return n;
    }

    private static int col_alias_no = 1;

    public static String genColumnAlias() {
        String n = String.format("vc%d", col_alias_no);
        col_alias_no++;
        return n;
    }

    private static int temp_tab_no = 1;

    public static TableUniqueName getTempTableName(VerdictContext vc) {
        String n = String.format("vt%d_%d", vc.getContextId() % 100, temp_tab_no);
        temp_tab_no++;
        return TableUniqueName.uname(vc, n);
    }

    public static TableUniqueName getTempTableName(VerdictContext vc, String schema) {
        String n = String.format("vt%d_%d", vc.getContextId() % 100, temp_tab_no);
        temp_tab_no++;
        return TableUniqueName.uname(schema, n);
    }

    public String partitionColumnName() {
        return vc.getDbms().partitionColumnName();
    }

    // /**
    // * Used when a universe sample is used for distinct-count.
    // * @return
    // */
    // public String distinctCountPartitionColumnName() {
    // return vc.getDbms().distinctCountPartitionColumnName();
    // }

    public String samplingProbabilityColumnName() {
        return vc.getDbms().samplingProbabilityColumnName();
    }

    public String samplingRatioColumnName() {
        return vc.getDbms().samplingRatioColumnName();
    }

    public static String errorBoundColumn(String original) {
        return String.format("%s_err", original);
    }

    protected static boolean areMatchingUniverseSamples(ApproxRelation r1, ApproxRelation r2,
            List<Pair<Expr, Expr>> joincond) {
        List<Expr> leftJoinCols = new ArrayList<Expr>();
        List<Expr> rightJoinCols = new ArrayList<Expr>();
        for (Pair<Expr, Expr> pair : joincond) {
            leftJoinCols.add(pair.getLeft());
            rightJoinCols.add(pair.getRight());
        }

        if (r1.sampleType().equals("universe") && r2.sampleType().equals("universe")) {
            List<String> cols1 = r1.getColumnsOnWhichSamplesAreCreated();
            List<String> cols2 = r2.getColumnsOnWhichSamplesAreCreated();
            if ((joinColumnsEqualToSampleColumns(leftJoinCols, cols1)
                    && joinColumnsEqualToSampleColumns(rightJoinCols, cols2)) ||
                (joinColumnsEqualToSampleColumns(leftJoinCols, cols2)
                        && joinColumnsEqualToSampleColumns(rightJoinCols, cols1))) {
                return true;
            }
        }
        return false;
    }

    private static boolean joinColumnsEqualToSampleColumns(List<Expr> joinCols, List<String> sampleColNames) {
        List<String> joinColNames = new ArrayList<String>();
        for (Expr expr : joinCols) {
            if (expr instanceof ColNameExpr) {
                joinColNames.add(((ColNameExpr) expr).getCol());
            }
        }
        return joinColNames.equals(sampleColNames);
    }

}

class TableNameReplacerInExpr extends ExprModifier {

    final protected Map<TableUniqueName, String> sub;

    public TableNameReplacerInExpr(VerdictContext vc, Map<TableUniqueName, String> sub) {
        super(vc);
        this.sub = sub;
    }

    public Expr call(Expr expr) {
        if (expr instanceof ColNameExpr) {
            return replaceColNameExpr((ColNameExpr) expr);
        } else if (expr instanceof FuncExpr) {
            return replaceFuncExpr((FuncExpr) expr);
        } else if (expr instanceof SubqueryExpr) {
            return replaceSubqueryExpr((SubqueryExpr) expr);
        } else if (expr instanceof OrderByExpr) {
            return replaceOrderByExpr((OrderByExpr) expr);
        } else {
            return expr;
        }
    }

    /**
     * Replaces if (1) schema name is omitted in the join condition and the table
     * name matches the table name of the original table (2) schema name is fully
     * specified in the join condition and both the schema and table names match the
     * original schema and table names.
     * 
     * @param expr
     * @return
     */
    protected Expr replaceColNameExpr(ColNameExpr expr) {
        if (expr.getTab() == null) {
            return expr;
        }

        TableUniqueName old = TableUniqueName.uname(expr.getSchema(), expr.getTab());

        if (old.getSchemaName() == null) {
            for (Map.Entry<TableUniqueName, String> entry : sub.entrySet()) {
                TableUniqueName original = entry.getKey();
                String subAlias = entry.getValue();
                if (original.getTableName().equals(expr.getTab())) {
                    return new ColNameExpr(vc, expr.getCol(), subAlias);
                }
            }
        } else {
            if (sub.containsKey(old)) {
                String rep = sub.get(old);
                return new ColNameExpr(vc, expr.getCol(), rep);
            }
        }
        // if nothing matched, then return the original expression.
        return expr;
    }

    protected Expr replaceSubqueryExpr(SubqueryExpr expr) {
        return expr;
    }

    protected Expr replaceFuncExpr(FuncExpr expr) {
        List<Expr> argument_exprs = expr.getExpressions();
        OverClause over = expr.getOverClause();
        List<Expr> new_argument_exprs = new ArrayList<Expr>();
        for (Expr e : argument_exprs) {
            new_argument_exprs.add(visit(e));
        }
        FuncExpr newExpr = new FuncExpr(expr.getFuncName(), new_argument_exprs, over);
        return newExpr;
    }

    protected Expr replaceOrderByExpr(OrderByExpr expr) {
        Expr e = expr.getExpression();
        return new OrderByExpr(expr.getVerdictContext(), visit(e), expr.getDirection().orNull());
    }
};

class PrettyPrintVisitor extends VerdictSQLBaseVisitor<String> {

    protected String sql;

    protected String indent = "";

    private VerdictContext vc;

    // pair of original table name and its alias
    // protected Map<String, String> tableAliases = new HashMap<String, String>();

    public PrettyPrintVisitor(VerdictContext vc, String sql) {
        this.vc = vc;
        this.sql = sql;
        this.indent = "";
    }

    public void setIndent(String indent) {
        this.indent = indent;
    }

    @Override
    public String visitCreate_table_as_select(VerdictSQLParser.Create_table_as_selectContext ctx) {
        String create = String.format("CREATE TABLE %s%s%s AS\n", (ctx.IF() != null) ? "IF NOT EXISTS " : "",
                ctx.table_name().getText(), (ctx.STORED_AS_PARQUET() != null) ? " STORED AS PARQUET" : "");

        PrettyPrintVisitor v = new PrettyPrintVisitor(vc, sql);
        v.setIndent(indent + "    ");
        String select = v.visit(ctx.select_statement());

        return create + select;
    }

    @Override
    public String visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
        StringBuilder query = new StringBuilder(1000);
        query.append(visit(ctx.query_expression()));

        if (ctx.order_by_clause() != null) {
            query.append("\n" + indent + visit(ctx.order_by_clause()));
        }

        if (ctx.limit_clause() != null) {
            query.append("\n" + indent + visit(ctx.limit_clause()));
        }
        return query.toString();
    }

    @Override
    public String visitQuery_specification(VerdictSQLParser.Query_specificationContext ctx) {
        // Construct a query string after processing all subqueries.
        // The processed subqueries are stored as a view.
        StringBuilder query = new StringBuilder(200);
        query.append(indent + "SELECT ");
        query.append(visit(ctx.select_list()));
        query.append(" ");

        query.append("\n" + indent + "FROM ");
        boolean isFirstTableSource = true;
        for (VerdictSQLParser.Table_sourceContext tctx : ctx.table_source()) {
            if (isFirstTableSource) {
                query.append(visit(tctx));
            } else {
                query.append(String.format(", %s", visit(tctx)));
            }
            isFirstTableSource = false;
        }
        // query.append(" ");

        if (ctx.where != null) {
            query.append("\n" + indent + "WHERE ");
            query.append(visit(ctx.where));
            query.append(" ");
        }

        if (ctx.group_by_item() != null && ctx.group_by_item().size() > 0) {
            query.append("\n" + indent + "GROUP BY ");
            List<String> groupby = new ArrayList<String>();
            for (VerdictSQLParser.Group_by_itemContext gctx : ctx.group_by_item()) {
                groupby.add(visit(gctx));
            }
            query.append(Joiner.on(", ").join(groupby));
        }

        String sql = query.toString();
        return sql;
    }

    boolean isFirstSelectElem = true;

    @Override
    public String visitSelect_list(VerdictSQLParser.Select_listContext ctx) {
        StringBuilder sql = new StringBuilder(500);
        int i = 0;
        for (VerdictSQLParser.Select_list_elemContext ectx : ctx.select_list_elem()) {
            if (i > 0) {
                sql.append(",\n" + indent + "       ");
            }

            // if (i > 0 && i%5 == 0) {
            // sql.append("\n" + indent + " ");
            // }

            sql.append(visit(ectx));
            i++;
        }
        return sql.toString();
    }

    @Override
    public String visitSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx) {
        if (ctx.getText().equals("*")) {
            return "*";
        } else {
            // StringBuilder elem = new StringBuilder();
            // elem.append(visit(ctx.expression()));
            // if (ctx.column_alias() != null) {
            // elem.append(String.format(" AS %s", ctx.column_alias().getText()));
            // }
            Expr expr = Expr.from(vc, ctx.expression());
            String alias = (ctx.column_alias() == null) ? null : ctx.column_alias().getText();
            return (new SelectElem(vc, expr, alias)).toString();
        }
    }

    @Override
    public String visitSearch_condition(VerdictSQLParser.Search_conditionContext ctx) {
        List<String> c = new ArrayList<String>();
        for (VerdictSQLParser.Search_condition_orContext octx : ctx.search_condition_or()) {
            c.add(visit(octx));
        }
        return Joiner.on(String.format("\n%s  AND ", indent)).join(c);
    }

    @Override
    public String visitSearch_condition_or(VerdictSQLParser.Search_condition_orContext ctx) {
        List<String> c = new ArrayList<String>();
        for (VerdictSQLParser.Search_condition_notContext nctx : ctx.search_condition_not()) {
            c.add(visit(nctx));
        }
        return Joiner.on(String.format("\n%s  OR ", indent)).join(c);
    }

    @Override
    public String visitSearch_condition_not(VerdictSQLParser.Search_condition_notContext ctx) {
        String predicate = visit(ctx.predicate());
        return ((ctx.NOT() != null) ? "NOT" : "") + predicate;
    }

    @Override
    public String visitSubquery(VerdictSQLParser.SubqueryContext ctx) {
        PrettyPrintVisitor v = new PrettyPrintVisitor(vc, sql);
        v.setIndent(indent + "     ");
        return v.visit(ctx.select_statement());
    }

    @Override
    public String visitExists_predicate(VerdictSQLParser.Exists_predicateContext ctx) {
        return String.format("EXISTS (\n%s)", visit(ctx.subquery()));
    }

    @Override
    public String visitComp_expr_predicate(VerdictSQLParser.Comp_expr_predicateContext ctx) {
        String exp1 = visit(ctx.expression(0));
        String exp2 = visit(ctx.expression(1));
        // String exp1 = Expr.from(ctx.expression(0)).toString();
        // Expr expr = Expr.from(ctx.expression(1));
        // String exp2 = Expr.from(ctx.expression(1)).toString();
        return String.format("%s %s %s", exp1, ctx.comparison_operator().getText(), exp2);
    }

    @Override
    public String visitSetcomp_expr_predicate(VerdictSQLParser.Setcomp_expr_predicateContext ctx) {
        return String.format("%s %s (\n%s)", ctx.expression().getText(), ctx.comparison_operator().getText(),
                visit(ctx.subquery()));
    }

    @Override
    public String visitComp_between_expr(VerdictSQLParser.Comp_between_exprContext ctx) {
        return String.format("%s %s BETWEEN %s AND %s", ctx.expression(0).getText(), (ctx.NOT() == null) ? "" : "NOT",
                ctx.expression(1).getText(), ctx.expression(2).getText());
    }

    @Override
    public String visitIs_predicate(VerdictSQLParser.Is_predicateContext ctx) {
        return String.format("%s IS %s%s", visit(ctx.expression()), (ctx.null_notnull().NOT() != null) ? "NOT " : "",
                "NULL");
    }

    private String getExpressions(VerdictSQLParser.Expression_listContext ctx) {
        List<String> e = new ArrayList<String>();
        for (VerdictSQLParser.ExpressionContext ectx : ctx.expression()) {
            e.add(visit(ectx));
        }
        return Joiner.on(", ").join(e);
    }

    @Override
    public String visitIn_predicate(VerdictSQLParser.In_predicateContext ctx) {
        return String.format("%s %s IN (%s)", ctx.expression().getText(), (ctx.NOT() == null) ? "" : "NOT",
                (ctx.subquery() == null) ? getExpressions(ctx.expression_list()) : "\n" + visit(ctx.subquery()));
    }

    @Override
    public String visitLike_predicate(VerdictSQLParser.Like_predicateContext ctx) {
        return String.format("%s %s LIKE %s", ctx.expression(0).getText(), (ctx.NOT() == null) ? "" : "NOT",
                ctx.expression(1).getText());
    }

    @Override
    public String visitCase_expr(VerdictSQLParser.Case_exprContext ctx) {
        StringBuilder sql = new StringBuilder();
        sql.append("CASE");
        List<ExpressionContext> exprs = ctx.expression();

        List<Search_conditionContext> search_conds = ctx.search_condition();
        if (search_conds.size() > 0) { // second case
            for (int i = 0; i < search_conds.size(); i++) {
                sql.append(" WHEN ");
                sql.append(visit(search_conds.get(i)));
                sql.append(" THEN ");
                sql.append(visit(exprs.get(i)));
            }

            if (exprs.size() > search_conds.size()) {
                sql.append(" ELSE ");
                sql.append(visit(exprs.get(exprs.size() - 1)));
            }
        } else { // first case
            sql.append(" ");
            sql.append(exprs.get(0));
            for (int j = 0; j < (exprs.size() - 1) / 2; j++) {
                int i1 = j * 2 + 1;
                int i2 = j * 2 + 2;
                sql.append(" WHEN ");
                sql.append(visit(exprs.get(i1)));
                sql.append(" THEN ");
                sql.append(visit(exprs.get(i2)));
            }
            if (ctx.ELSE() != null) {
                sql.append(" ELSE ");
                sql.append(exprs.get(exprs.size() - 1));
            }
        }

        sql.append(" END");
        return sql.toString();
    }

    @Override
    public String visitBracket_expression(VerdictSQLParser.Bracket_expressionContext ctx) {
        return "(" + visit(ctx.expression()) + ")";
    }

    @Override
    public String visitBracket_predicate(VerdictSQLParser.Bracket_predicateContext ctx) {
        return String.format("(%s)", visit(ctx.search_condition()));
    }

    @Override
    public String visitPrimitive_expression(VerdictSQLParser.Primitive_expressionContext ctx) {
        return ctx.getText();
    }

    @Override
    public String visitColumn_ref_expression(VerdictSQLParser.Column_ref_expressionContext ctx) {
        return visit(ctx.full_column_name());
    }

    @Override
    public String visitFull_column_name(VerdictSQLParser.Full_column_nameContext ctx) {
        StringBuilder tabName = new StringBuilder();
        if (ctx.table_name() != null) {
            tabName.append(String.format("%s.", visit(ctx.table_name())));
        }
        tabName.append(ctx.column_name().getText());
        return tabName.toString();
    }

    @Override
    public String visitFunction_call_expression(VerdictSQLParser.Function_call_expressionContext ctx) {
        return FuncExpr.from(vc, ctx.function_call()).toString();
    }

    // @Override public String
    // visitMathematical_function_expression(VerdictSQLParser.Mathematical_function_expressionContext
    // ctx)
    // {
    // if (ctx.expression() != null) {
    // return String.format("%s(%s)", ctx.unary_mathematical_function().getText(),
    // visit(ctx.expression()));
    // } else {
    // return String.format("%s()", ctx.noparam_mathematical_function().getText());
    // }
    // }

    @Override
    public String visitUnary_manipulation_function(VerdictSQLParser.Unary_manipulation_functionContext ctx) {
        return String.format("%s(%s)", ctx.getText(), visit(ctx.expression()));
    }

    @Override
    public String visitNoparam_manipulation_function(VerdictSQLParser.Noparam_manipulation_functionContext ctx) {
        return String.format("%s()", ctx.getText());
    }

    @Override
    public String visitBinary_manipulation_function(VerdictSQLParser.Binary_manipulation_functionContext ctx) {
        return String.format("%s(%s, %s)", ctx.getText(), visit(ctx.expression(0)), visit(ctx.expression(1)));
    }

    @Override
    public String visitAggregate_windowed_function(VerdictSQLParser.Aggregate_windowed_functionContext ctx) {
        if (ctx.AVG() != null) {
            return String.format("AVG(%s)", visit(ctx.all_distinct_expression()));
        } else if (ctx.SUM() != null) {
            return String.format("SUM(%s)", visit(ctx.all_distinct_expression()));
        } else if (ctx.COUNT() != null) {
            if (ctx.all_distinct_expression() != null) {
                String colName = ctx.all_distinct_expression().expression().getText();
                if (ctx.all_distinct_expression().DISTINCT() != null) {
                    return String.format("COUNT(DISTINCT %s)", colName);
                } else {
                    return String.format("COUNT(%s)", colName);
                }
            } else {
                return String.format("COUNT(*)");
            }
        } else if (ctx.NDV() != null) {
            return String.format("NDV(%s)", visit(ctx.all_distinct_expression()));
        }
        VerdictLogger.error(this, String.format("Unexpected aggregate function expression: %s", ctx.getText()));
        return null; // we don't handle other aggregate functions for now.
    }

    @Override
    public String visitBinary_operator_expression(VerdictSQLParser.Binary_operator_expressionContext ctx) {
        VerdictSQLParser.ExpressionContext left = ctx.expression(0);
        VerdictSQLParser.ExpressionContext right = ctx.expression(1);
        String op = ctx.op.getText();

        String leftCol = visit(left);
        String rightCol = visit(right);

        if (leftCol == null || rightCol == null)
            return null;
        else if (op.equals("*"))
            return leftCol + " * " + rightCol;
        else if (op.equals("+"))
            return leftCol + " + " + rightCol;
        else if (op.equals("/"))
            return leftCol + " / " + rightCol;
        else if (op.equals("-"))
            return leftCol + " - " + rightCol;
        else if (op.equals("%"))
            return leftCol + " % " + rightCol;
        else
            return null;
    }

    @Override
    public String visitSubquery_expression(VerdictSQLParser.Subquery_expressionContext ctx) {
        return "(\n" + visit(ctx.subquery()) + ")";
    }

    @Override
    public String visitTable_source(VerdictSQLParser.Table_sourceContext ctx) {
        return visit(ctx.table_source_item_joined());
    }

    @Override
    public String visitTable_source_item_joined(VerdictSQLParser.Table_source_item_joinedContext ctx) {
        StringBuilder sql = new StringBuilder();
        sql.append(visit(ctx.table_source_item()));
        for (Join_partContext jctx : ctx.join_part()) {
            sql.append(visit(jctx));
        }
        return sql.toString();
    }

    @Override
    public String visitJoin_part(VerdictSQLParser.Join_partContext ctx) {
        if (ctx.INNER() != null) {
            return "\n" + indent + "     " + String.format("INNER JOIN %s ", visit(ctx.table_source())) + "\n" + indent
                    + "     " + String.format("ON %s", visit(ctx.search_condition()));
        } else if (ctx.OUTER() != null) {
            return "\n" + indent + "     " + String.format("%s OUTER JOIN %s ON %s", ctx.join_type.getText(),
                    visit(ctx.table_source()), visit(ctx.search_condition()));
        } else if (ctx.CROSS() != null) {
            return "\n" + indent + "     " + String.format("CROSS JOIN %s", visit(ctx.table_source()));
        } else {
            return "\n" + indent + "     " + String.format("UNSUPPORTED JOIN (%s)", ctx.getText());
        }
    }

    @Override
    public String visitSample_table_name_item(VerdictSQLParser.Sample_table_name_itemContext ctx) {
        assert (false); // specifying sample table size is not supported now.
        return visitChildren(ctx);
    }

    @Override
    public String visitHinted_table_name_item(VerdictSQLParser.Hinted_table_name_itemContext ctx) {
        String tableNameItem = visit(ctx.table_name_with_hint());
        if (ctx.as_table_alias() == null) {
            return tableNameItem;
        } else {
            String alias = ctx.as_table_alias().table_alias().getText();
            return tableNameItem + " " + alias;
        }
    }

    @Override
    public String visitDerived_table_source_item(VerdictSQLParser.Derived_table_source_itemContext ctx) {
        return String.format("(\n%s) %s", visit(ctx.derived_table().subquery()),
                ctx.as_table_alias().table_alias().getText());
    }

    @Override
    public String visitTable_name_with_hint(VerdictSQLParser.Table_name_with_hintContext ctx) {
        return visit(ctx.table_name());
    }

    @Override
    public String visitTable_name(VerdictSQLParser.Table_nameContext ctx) {
        return ctx.getText();
    }

    @Override
    public String visitGroup_by_item(VerdictSQLParser.Group_by_itemContext ctx) {
        return Expr.from(vc, ctx.expression()).toSql();
    }

    @Override
    public String visitOrder_by_clause(VerdictSQLParser.Order_by_clauseContext ctx) {
        return getOriginalText(ctx);
    }

    @Override
    public String visitLimit_clause(VerdictSQLParser.Limit_clauseContext ctx) {
        return "LIMIT " + ctx.number().getText();
    }

    protected String getOriginalText(ParserRuleContext ctx) {
        int a = ctx.start.getStartIndex();
        int b = ctx.stop.getStopIndex();
        Interval interval = new Interval(a, b);
        return (new ANTLRInputStream(sql)).getText(interval);
    }
}
