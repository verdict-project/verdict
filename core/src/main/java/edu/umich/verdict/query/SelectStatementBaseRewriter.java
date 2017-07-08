package edu.umich.verdict.query;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;

import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.VerdictSQLParser.Join_partContext;
import edu.umich.verdict.util.VerdictLogger;

/**
 * A default query parser that returns an identical select query statement.
 * @author Yongjoo Park
 *
 */
public class SelectStatementBaseRewriter extends VerdictSQLBaseVisitor<String> {
	
	protected String queryString;
	
	protected String err_msg;
	
	protected int depth = 0;	// depth increases whenever encouters a subquery.
	
	protected int defaultIndent = 0;
	
	protected String indentString = "";
	
	// pair of original table name and its alias
//	protected Map<String, String> tableAliases = new HashMap<String, String>();
	
	public SelectStatementBaseRewriter(String queryString) {
		this.queryString = queryString;
	}
	
	public void setIndentLevel(int level) {
		this.defaultIndent = level;
		this.indentString = new String(new char[defaultIndent]).replace("\0", " ");
	}
	
	public void setDepth(int depth) {
		this.depth = depth;
	}
	
	protected String tableSourceReplacer(String originalTableName) {
		return originalTableName;
	}
	
	protected String tableNameReplacer(String originalTableName) {
		return originalTableName;
	}
	
	protected String getOriginalText(ParserRuleContext ctx) {
		int a = ctx.start.getStartIndex();
	    int b = ctx.stop.getStopIndex();
	    Interval interval = new Interval(a,b);
	    return CharStreams.fromString(queryString).getText(interval);
	}
	
	@Override
	protected String aggregateResult(String aggregate, String nextResult) {
		if (aggregate != null) return aggregate;
		else if (nextResult != null) return nextResult;
		else return null;
	}
	
	@Override
	public String visitCreate_table_as_select(VerdictSQLParser.Create_table_as_selectContext ctx) {
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE TABLE");
		if (ctx.IF() != null) sql.append(" IF NOT EXISTS");
		sql.append(String.format(" %s AS ", ctx.table_name().getText()));
		sql.append(visit(ctx.select_statement()));
		return sql.toString();
	}
	
	@Override
	public String visitCreate_view(VerdictSQLParser.Create_viewContext ctx) {
		StringBuilder sql = new StringBuilder();
		sql.append("CREATE VIEW");
		sql.append(String.format(" %s AS ", ctx.view_name().getText()));
		sql.append(visit(ctx.select_statement()));
		return sql.toString();
	}
	
	@Override
	public String visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
		StringBuilder query = new StringBuilder(1000);
		query.append(visit(ctx.query_expression()));
		
		if (ctx.order_by_clause() != null) {
			query.append(String.format(" %s", visit(ctx.order_by_clause())));
		}
		
		if (ctx.limit_clause() != null) {
			query.append(String.format(" %s", visit(ctx.limit_clause())));
		}
		return query.toString();
	}
	
	@Override
	public String visitQuery_specification(VerdictSQLParser.Query_specificationContext ctx) {
		// Construct a query string after processing all subqueries.
		// The processed subqueries are stored as a view.
		StringBuilder query = new StringBuilder(200);
		query.append("\n" + indentString + "SELECT ");
		query.append(visit(ctx.select_list()));
		query.append(" ");
		
		query.append("\n" + indentString + "FROM ");
		boolean isFirstTableSource = true;
		for (VerdictSQLParser.Table_sourceContext tctx : ctx.table_source()) {
			if (isFirstTableSource) {
				query.append(visit(tctx));
			} else {
				query.append(String.format(", %s", visit(tctx)));
			}
			isFirstTableSource = false;
		}
		query.append(" ");
		
		if (ctx.where != null) {
			query.append("\n" + indentString + "WHERE ");
			query.append(visit(ctx.where));
			query.append(" ");
		}
		
		if (ctx.group_by_item() != null && ctx.group_by_item().size() > 0) {
			query.append("\n" + indentString + "GROUP BY ");
			for (VerdictSQLParser.Group_by_itemContext gctx : ctx.group_by_item()) {
				query.append(visit(gctx));
			}
			query.append(" ");
		}
		
		String sql = query.toString();
		return sql;
	}
	
	boolean isFirstSelectElem = true;
	
	@Override
	public String visitSelect_list(VerdictSQLParser.Select_listContext ctx) {
		isFirstSelectElem = true;
		StringBuilder sql = new StringBuilder();
		for (VerdictSQLParser.Select_list_elemContext ectx : ctx.select_list_elem()) {
			if (isFirstSelectElem) {
				isFirstSelectElem = false;
				sql.append(visit(ectx));
			}
			else {
				sql.append(String.format(", %s", visit(ectx)));
			}
		}
		return sql.toString();
	}
	
	@Override
	public String visitSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx) {
		if (ctx.getText().equals("*")) {
			return "*";
		} else {
			StringBuilder elem = new StringBuilder();
			elem.append(visit(ctx.expression()));
			if (ctx.column_alias() != null) {
				elem.append(String.format(" AS %s", ctx.column_alias().getText()));
			}
			return elem.toString();
		}
	}
	
	// search conditions
	@Override
	public String visitSearch_condition(VerdictSQLParser.Search_conditionContext ctx) {
		StringBuilder query = new StringBuilder(200);
		boolean isFirst = true;
		for (VerdictSQLParser.Search_condition_orContext octx : ctx.search_condition_or()) {
			if (isFirst) isFirst = false;
			else         query.append(String.format("\n%s  AND ", indentString));
			query.append(visit(octx));
		}
		return query.toString();
	}

	@Override
	public String visitSearch_condition_or(VerdictSQLParser.Search_condition_orContext ctx) {
		StringBuilder query = new StringBuilder(200);
		boolean isFirst = true;
		for (VerdictSQLParser.Search_condition_notContext nctx : ctx.search_condition_not()) {
			if (isFirst) isFirst = false;
			else         query.append(" OR ");
			query.append(visit(nctx));
		}
		return query.toString();
	}

	@Override
	public String visitSearch_condition_not(VerdictSQLParser.Search_condition_notContext ctx) {
		String predicate = visit(ctx.predicate());
		return ((ctx.NOT() != null) ? "NOT" : "") + predicate;   
	}
	
	@Override
	public String visitSubquery(VerdictSQLParser.SubqueryContext ctx) {
		depth++;
		String ret = visit(ctx.select_statement());
		depth--;
		return ret;
	}
	
	@Override
	public String visitExists_predicate(VerdictSQLParser.Exists_predicateContext ctx) {
		return String.format("EXISTS (\n%s)", visit(ctx.subquery()));
	}
	
	@Override
	public String visitComp_expr_predicate(VerdictSQLParser.Comp_expr_predicateContext ctx) {
		String exp1 = visit(ctx.expression(0));
		String exp2 = visit(ctx.expression(1));
		return String.format("%s %s %s", exp1, ctx.comparison_operator().getText(), exp2);
	}
	
	@Override
	public String visitSetcomp_expr_predicate(VerdictSQLParser.Setcomp_expr_predicateContext ctx) {
		return String.format("%s %s (\n%s)", ctx.expression().getText(), ctx.comparison_operator().getText(), visit(ctx.subquery()));
	}
	
	@Override
	public String visitComp_between_expr(VerdictSQLParser.Comp_between_exprContext ctx) {
		return String.format("%s %s BETWEEN %s AND %s",
				ctx.expression(0).getText(), (ctx.NOT() == null)? "" : "NOT", ctx.expression(1).getText(), ctx.expression(2).getText() );
	}
	
	@Override
	public String visitIs_predicate(VerdictSQLParser.Is_predicateContext ctx) {
		return String.format("%s IS %s%s", visit(ctx.expression()), (ctx.null_notnull().NOT() != null) ? "NOT " : "", "NULL");
	}
	
	private String getExpressions(VerdictSQLParser.Expression_listContext ctx) {
		StringBuilder query = new StringBuilder(200);
		boolean isFirst = true;
		for (VerdictSQLParser.ExpressionContext ectx : ctx.expression()) {
			if (!isFirst) query.append(", ");
			query.append(visit(ectx));
		}
		return query.toString();
	}
	
	@Override
	public String visitIn_predicate(VerdictSQLParser.In_predicateContext ctx) {
		return 	String.format("%s %s IN (%s)",
				ctx.expression().getText(),
				(ctx.NOT() == null)? "" : "NOT",
				(ctx.subquery() == null)? getExpressions(ctx.expression_list()) : "\n" + visit(ctx.subquery()));
	}
	
	@Override
	public String visitLike_predicate(VerdictSQLParser.Like_predicateContext ctx) {
		return String.format("%s %s LIKE %s",
				ctx.expression(0).getText(), (ctx.NOT() == null)? "" : "NOT", ctx.expression(1).getText());
	}
	
	@Override
	public String visitBracket_predicate(VerdictSQLParser.Bracket_predicateContext ctx) {
		return String.format("(%s)", visit(ctx.search_condition()));
	}
	
	// expressions
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
	
//	@Override
//	public String visitFunction_call(VerdictSQLParser.Function_callContext ctx) {
//		return visit(ctx.aggregate_windowed_function());
//	}
	
//	@Override public String visitMathematical_function_expression(VerdictSQLParser.Mathematical_function_expressionContext ctx)
//	{
//		return String.format("%s(%s)", ctx.unary_mathematical_function().getText(), visit(ctx.expression()));
//	}
	
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
		}
		VerdictLogger.error(this, String.format("Unexpected aggregate function expression: %s", ctx.getText()));
		return null;	// we don't handle other aggregate functions for now.
		
	}
	
	@Override
	public String visitBinary_operator_expression(VerdictSQLParser.Binary_operator_expressionContext ctx) {
		VerdictSQLParser.ExpressionContext left = ctx.expression(0);
		VerdictSQLParser.ExpressionContext right = ctx.expression(1);
		String op = ctx.op.getText();
		
		String leftCol = visit(left);
		String rightCol = visit(right);
		
		if (leftCol == null || rightCol == null) return null;
		else if (op.equals("*")) return leftCol + "*" + rightCol;
		else if (op.equals("+")) return leftCol + "+" + rightCol;
		else if (op.equals("/")) return leftCol + "/" + rightCol;
		else if (op.equals("-")) return leftCol + "-" + rightCol;
		else return null;
	}
	
	@Override
	public String visitSubquery_expression(VerdictSQLParser.Subquery_expressionContext ctx) {
		return String.format("(\n%s)", visit(ctx.subquery()));
	}
	
	// from clauses
	protected boolean withinFromClause = false;
	
	@Override
	public String visitTable_source(VerdictSQLParser.Table_sourceContext ctx) {
		withinFromClause = true;
		String from = visit(ctx.table_source_item_joined());
		withinFromClause = false;
		return from;
	}
		
	@Override
	public String visitTable_source_item_joined(VerdictSQLParser.Table_source_item_joinedContext ctx) {
		StringBuilder sql = new StringBuilder();
		sql.append(visit(ctx.table_source_item()));
		for (Join_partContext jctx : ctx.join_part()) {
			sql.append(String.format(" %s", visit(jctx)));
		}
		return sql.toString();
	}
	
	@Override
	public String visitSample_table_name_item(VerdictSQLParser.Sample_table_name_itemContext ctx) {
		assert(false);		// specifying sample table size is not supported now.
		return visitChildren(ctx);
	}
	
	@Override
	public String visitHinted_table_name_item(VerdictSQLParser.Hinted_table_name_itemContext ctx) {
		String tableNameItem = visit(ctx.table_name_with_hint());
		if (ctx.as_table_alias() == null) {
			return tableNameItem;
		} else {
			String alias = ctx.as_table_alias().getText();
			return tableNameItem + " " + alias;
		}
	}
	
	@Override
	public String visitDerived_table_source_item(VerdictSQLParser.Derived_table_source_itemContext ctx) {
		return String.format("(\n%s) %s ", visit(ctx.derived_table().subquery()), ctx.as_table_alias().getText());
	}
	
	@Override
	public String visitTable_name_with_hint(VerdictSQLParser.Table_name_with_hintContext ctx) {
		return visit(ctx.table_name());
	}
	
	@Override
	public String visitTable_name(VerdictSQLParser.Table_nameContext ctx) {
		if (withinFromClause) {
			return tableSourceReplacer(ctx.getText());
		} else {
			return tableNameReplacer(ctx.getText());
		} 
	}
	
	// group by
	
	protected boolean isFirstGroup = true;
	
	@Override
	public String visitGroup_by_item(VerdictSQLParser.Group_by_itemContext ctx) {
		if (isFirstGroup) {
			String groupName = ctx.getText();
			isFirstGroup = false;
			return groupName;
		}
		else {
			return ", " + ctx.getText();
		}
	}
	
	@Override
	public String visitOrder_by_clause(VerdictSQLParser.Order_by_clauseContext ctx) {
		StringBuilder orderby = new StringBuilder();
		orderby.append("\n" + indentString + "ORDER BY");
		boolean isFirst = true;
		for (VerdictSQLParser.Order_by_expressionContext octx : ctx.order_by_expression()) {
			orderby.append(String.format(
					(isFirst)? " %s" : ", %s",
					visit(octx.expression())));
			if (octx.DESC() != null) {
				orderby.append(" DESC");
			}
			if (octx.ASC() != null) {
				orderby.append(" ASC");
			}
			isFirst = false;
		}
		return orderby.toString();
	}
	
	@Override
	public String visitLimit_clause(VerdictSQLParser.Limit_clauseContext ctx) {
		return "\n" + indentString + getOriginalText(ctx);
	}
}
