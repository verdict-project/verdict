package edu.umich.verdict.query;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.datatypes.VerdictResultSet;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.exceptions.VerdictQuerySyntaxException;
import edu.umich.verdict.util.VerdictLogger;

public class VerdictApproximateSelectQuery extends VerdictSelectQuery {

	public VerdictApproximateSelectQuery(String queryString, VerdictContext vc) {
		super(queryString, vc);
	}

	public VerdictApproximateSelectQuery(VerdictQuery parent) {
		super(parent);
	}

	public static boolean doesSupport(String queryString) {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(queryString));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		
		final HashSet<String> aggs = new HashSet<String>();
		VerdictSQLBaseVisitor<String> visitor = new VerdictSQLBaseVisitor<String>() {
			@Override
			public String visitSubquery(VerdictSQLParser.SubqueryContext ctx) {
				return null;	// ignore subqueries.
			}
			
			@Override
			public String visitAggregate_windowed_function(VerdictSQLParser.Aggregate_windowed_functionContext ctx) {
				if (ctx.AVG() != null || ctx.COUNT() != null || ctx.SUM() != null) {
					aggs.add("1");	// indicate a supported-function is included.
				}
				else {
					aggs.add("0");	// indicate non-supported function is included.
				}
				return null;
			}
		};
		visitor.visit(p.query_expression().query_specification().select_list());
		
		return (aggs.size() == 1) && aggs.contains("1");
	}
	
	@Override
	public ResultSet compute() throws VerdictException {
		Pair<String, VerdictApproximateSelectStatementVisitor> rewrittenQueryAndVisitor = rewriteQuery();
		
		String rewrittenQuery = rewrittenQueryAndVisitor.getLeft();
		List<Boolean> AggregateColumnIndicator = rewrittenQueryAndVisitor.getRight().getAggregateColumnIndicator();
		Map<TableUniqueName, TableUniqueName> replacedTables = rewrittenQueryAndVisitor.getRight().getCumulativeSampleTables();
		
		VerdictLogger.debug(this, "The input query was rewritten to:");
		VerdictLogger.debugPretty(this, rewrittenQuery, "  ");
		
		for (TableUniqueName original : replacedTables.keySet()) {
			VerdictLogger.info(String.format("Verdict is using a sample table for %s", original));
		}
		
		ResultSet rs = new VerdictResultSet(
				vc.getDbms().executeQuery(rewrittenQuery),
				convertAggColumnToDefaultError(AggregateColumnIndicator, replacedTables));
		return rs;
	}
	
	// TODO: this will not be used in the future.
	private List<Double> convertAggColumnToDefaultError(List<Boolean> AggregateColumnIndicator,
			Map<TableUniqueName, TableUniqueName> replacedTables) {
		List<Double> errors = new ArrayList<Double>();
		for (Boolean ind : AggregateColumnIndicator) {
			errors.add((ind && replacedTables.size() > 0)? 0.05 : 0.0);
		}
		return errors;
	}
	
	protected Pair<String, VerdictApproximateSelectStatementVisitor> rewriteQuery() throws VerdictException {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(queryString));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		
		VerdictApproximateSelectStatementVisitor queryRewriter = new VerdictApproximateSelectStatementVisitor(vc, queryString);
		String rewrittenQuery = queryRewriter.visit(p.select_statement());
		if (queryRewriter.getException() != null) {
			throw queryRewriter.getException();
		}
		return Pair.of(rewrittenQuery, queryRewriter);
	}
}


class VerdictApproximateSelectStatementVisitor extends VerdictSelectStatementBaseVisitor  {
	
	private VerdictContext vc;
	
	private VerdictQuerySyntaxException e;
	
	protected ArrayList<Boolean> aggColumnIndicator;
	
	public VerdictApproximateSelectStatementVisitor(VerdictContext vc, String queryString) {
		super(queryString);
		this.vc = vc;
		this.e = null;
		aggColumnIndicator = new ArrayList<Boolean>();
	}
	
	public VerdictQuerySyntaxException getException() {
		return e;
	}
	
	public List<Boolean> getAggregateColumnIndicator() {
		return aggColumnIndicator;
	}
	
	// This field stores the tables sources of only current level. That is, does not store the table sources
	// of the subqueries.
	// Note: currently, this field does not containi derived fields.
	private final ArrayList<TableUniqueName> replacedTableSources = new ArrayList<TableUniqueName>();
	
	// This field stores the replaced table sources of all levels.
	// left is the original table name, and the right is the replaced table name.
	private final Map<TableUniqueName, TableUniqueName> cumulativeReplacedTableSources = new TreeMap<TableUniqueName, TableUniqueName>();
	
	public Map<TableUniqueName, TableUniqueName> getCumulativeSampleTables() {
		return cumulativeReplacedTableSources;
	}
	
	private double sampleSizeToOriginalTableSizeRatio = -1;
	
	private double getSampleSizeToOriginalTableSizeRatio() {
		if (sampleSizeToOriginalTableSizeRatio == -1) {
			double sampleToOriginalRatio = 1.0;		// originalTableSize / sampleSize
			for (TableUniqueName t : replacedTableSources) {
				Pair<Long,Long> sampleAndOriginalSize =
						vc.getMeta().getSampleAndOriginalTableSizeByOriginalTableNameIfExists(t);
				double sampleSize = (double) sampleAndOriginalSize.getLeft();
				double originalTableSize = (double) sampleAndOriginalSize.getRight();
				
				sampleToOriginalRatio *= originalTableSize / sampleSize;
				VerdictLogger.debug(this, String.format("%s size ratio. sample size: %f, original size: %f", t, sampleSize, originalTableSize));
			}
			sampleSizeToOriginalTableSizeRatio = sampleToOriginalRatio;
		}
		return sampleSizeToOriginalTableSizeRatio;
	}
	
	@Override
	protected String tableSourceReplacer(String originalTableName) {
		TableUniqueName uTableName = TableUniqueName.uname(vc, originalTableName);
		TableUniqueName newTableSource = vc.getMeta().getSampleTableNameIfExistsElseOriginal(uTableName);
		replacedTableSources.add(uTableName);
		// note: newTableSource might be same as uTableName if there's no sample table exists.
		if (!uTableName.equals(newTableSource)) {
			cumulativeReplacedTableSources.put(uTableName, newTableSource);
		}
		return newTableSource.toString();
	}
	
	@Override
	protected String tableNameReplacer(String originalTableName) {
		return vc.getMeta().getSampleTableNameIfExistsElseOriginal(
				TableUniqueName.uname(vc, originalTableName)).toString();
	}
	
	@Override
	public String visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
		StringBuilder query = new StringBuilder(1000);
		query.append(visit(ctx.query_expression()));
		
		if (ctx.order_by_clause() != null) {
			query.append(String.format("\n%s", indentString + visit(ctx.order_by_clause())));
		}
		
		if (ctx.limit_clause() != null) {
			query.append(String.format("\n%s", indentString + visit(ctx.limit_clause())));
		}
		return query.toString();
	}
	
	private int select_list_elem_num = 0;	// 1 for the first column, 2 for the second column, and so on.
	
	private String quoteString() {
		return vc.getDbms().getQuoteString();
	}
	
	@Override
	public String visitSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx) {
		select_list_elem_num++;
		
		if (ctx.getText().equals("*")) {
			// TODO: replace * with all columns in the (joined) source table.
			return "*";
		} else {
			StringBuilder elem = new StringBuilder();
			elem.append(visit(ctx.expression()));
			
			if (ctx.column_alias() != null) {
				elem.append(String.format(" AS %s", ctx.column_alias().getText()));
			} else {
				if (depth != 0) {
					String msg = "An aggregate expression in subqueries must have an alias.";
					VerdictLogger.error(this, msg);
					e = new VerdictQuerySyntaxException(msg);
				} else {
					// We don't want to expose our rewritten expression
					elem.append(String.format(" AS %s%s%s", quoteString(), ctx.getText(), quoteString()));
				}
			}
			
			return elem.toString();
		}
	}
	
	@Override
	public String visitAggregate_windowed_function(VerdictSQLParser.Aggregate_windowed_functionContext ctx) {
		while (aggColumnIndicator.size() < select_list_elem_num-1) {
			aggColumnIndicator.add(false);		// pad zero
		}
		aggColumnIndicator.add(true);			// TODO this must be adapted according to the sample size.
		
		if (ctx.AVG() != null) {
			return String.format("AVG(%s)", visit(ctx.all_distinct_expression()), getSampleSizeToOriginalTableSizeRatio());
		} else if (ctx.SUM() != null) {
			return String.format("(SUM(%s) * %f)", visit(ctx.all_distinct_expression()), getSampleSizeToOriginalTableSizeRatio());
		} else if (ctx.COUNT() != null) {
			return String.format("ROUND((COUNT(*) * %f))", getSampleSizeToOriginalTableSizeRatio());
		}
		VerdictLogger.error(this, String.format("Unexpected aggregate function expression: %s", ctx.getText()));
		return null;	// we don't handle other aggregate functions for now.
	}
	
	@Override
	public String visitQuery_specification(VerdictSQLParser.Query_specificationContext ctx) {
		// FROM clause
		// We process the FROM clause first to get the ratio between the samples (if we use them) and the
		// original tables.
		StringBuilder fromClause = new StringBuilder(200);
		fromClause.append("FROM ");
		boolean isFirstTableSource = true;
		for (VerdictSQLParser.Table_sourceContext tctx : ctx.table_source()) {
			if (isFirstTableSource) {
				fromClause.append(visit(tctx));
			} else {
				fromClause.append(String.format(", %s", visit(tctx)));
			}
			isFirstTableSource = false;
		}
		
		// SELECT list
		StringBuilder selectList = new StringBuilder(200);
		selectList.append("SELECT ");
		selectList.append(visit(ctx.select_list()));
		
		// WHERE clause
		StringBuilder whereClause = new StringBuilder(200);
		if (ctx.where != null) {
			whereClause.append("WHERE ");
			whereClause.append(visit(ctx.where));
		}
		
		// Others
		StringBuilder otherClause = new StringBuilder(200);
		if (ctx.group_by_item() != null && ctx.group_by_item().size() > 0) {
			otherClause.append("GROUP BY ");
			for (VerdictSQLParser.Group_by_itemContext gctx : ctx.group_by_item()) {
				otherClause.append(visit(gctx));
			}
		}
		
		StringBuilder query = new StringBuilder(1000);
		query.append(indentString); query.append(selectList.toString());
		query.append("\n" + indentString); query.append(fromClause.toString());
		if (whereClause.length() > 0) { query.append("\n"); query.append(indentString); query.append(whereClause.toString()); }
		if (otherClause.length() > 0) {  query.append("\n"); query.append(indentString); query.append(otherClause.toString()); }
		return query.toString();
	}
	
	@Override
	public String visitSubquery(VerdictSQLParser.SubqueryContext ctx) {
		depth++;
		VerdictApproximateSelectStatementVisitor subqueryVisitor = new VerdictApproximateSelectStatementVisitor(vc, queryString);
		subqueryVisitor.setIndentLevel(defaultIndent + 4);
		String ret = subqueryVisitor.visit(ctx.select_statement());
		cumulativeReplacedTableSources.putAll(subqueryVisitor.getCumulativeSampleTables());
		depth--;
		return ret;
	}
	
};
