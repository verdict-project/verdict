package edu.umich.verdict.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.datatypes.Alias;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictQuerySyntaxException;
import edu.umich.verdict.util.NameHelpers;
import edu.umich.verdict.util.TypeCasting;
import edu.umich.verdict.util.VerdictLogger;

class AnalyticSelectStatementRewriter extends SelectStatementBaseRewriter  {
	
	protected VerdictContext vc;
	
	protected VerdictQuerySyntaxException e;
	
	protected List<Boolean> aggColumnIndicator;
	
	// Alias propagation
	// 1. we declare aliases for all columns.
	// 2. the alias of a non-aggregate column can simply be the column name itself.
	// 3. the alias of an aggregate column is an automatically generalized name (using genAlias()) unless its alias
	//    is explicitly specified.
	// 4. all declared aliases are stored in this field so that an outer query can access.
	// 5. the displayed column labels should be properly handled by VerdictResultSet class.
	protected List<Pair<String, Alias>> colName2Aliases;
	
	// Records the alias information from the derived tables; we will need this to replace
	// The key is the alias name of a derived table.
	// The value is the map of original column name and its Aliases (i.e., the original col name and its alias).
	protected Map<String, Map<String, Alias>> derivedTableColName2Aliases;
	
	// This field stores the tables sources of only current level. That is, does not store the table sources
	// of the subqueries.
	// Note: currently, this field does not contain derived fields.
	// TODO: this may be replaced with a single double value that indicates the sampling probability.
	protected final ArrayList<TableUniqueName> replacedTableSources = new ArrayList<TableUniqueName>();

	// This field stores the replaced table sources of all levels.
	// left is the original table name, and the right is the replaced table name.
	protected final Map<TableUniqueName, TableUniqueName> cumulativeReplacedTableSources = new TreeMap<TableUniqueName, TableUniqueName>();
	
	// For every table source, we remember the table name and its alias.
	// This info is used in the other clauses (such as where clause and select list) for replacing the table names with
	// their proper aliases.
	protected Map<TableUniqueName, Alias> tableAliases = new HashMap<TableUniqueName, Alias>();
	
	// Records the column index of a mean aggregation and the column index for its error.
	// If the right value is 0, it means there's no error info column.
	// All valid column indexes are not smaller than 1 (namely, one-indexed).
	protected Map<Integer, Integer> meanColIndex2ErrColIndex;

	
	public AnalyticSelectStatementRewriter(VerdictContext vc, String queryString) {
		super(queryString);
		this.vc = vc;
		this.e = null;
		aggColumnIndicator = new ArrayList<Boolean>();
		colName2Aliases = new ArrayList<Pair<String, Alias>>();
		meanColIndex2ErrColIndex = new HashMap<Integer, Integer>();
		derivedTableColName2Aliases = new HashMap<String, Map<String, Alias>>(); 
	}
	
	public VerdictQuerySyntaxException getException() {
		return e;
	}
	
	public List<Boolean> getAggregateColumnIndicator() {
		return aggColumnIndicator;
	}
	
	public List<Pair<String, Alias>> getColName2Aliases() {
		return colName2Aliases;
	}
	
	public Map<TableUniqueName, TableUniqueName> getCumulativeSampleTables() {
		return cumulativeReplacedTableSources;
	}
	
	public boolean isAggregateColumn(int columnIndex) {
		if (aggColumnIndicator.size() >= columnIndex && aggColumnIndicator.get(columnIndex-1)) {
			return aggColumnIndicator.get(columnIndex-1);
		} else {
			return false;
		}
	}
	
	public Map<Integer, Integer> getMean2ErrorColumnMap() {
		return meanColIndex2ErrColIndex;
	}
	
	protected double sampleSizeToOriginalTableSizeRatio = -1;
	
	protected double getSampleSizeToOriginalTableSizeRatio() {
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
	public String visitHinted_table_name_item(VerdictSQLParser.Hinted_table_name_itemContext ctx) {
		String tableNameItem = visit(ctx.table_name_with_hint());
		Alias alias = null;
		if (ctx.as_table_alias() == null) {
			alias = Alias.genAlias(depth, tableNameItem);
		} else {
			alias = new Alias(tableNameItem, ctx.as_table_alias().getText());
		}
		tableAliases.put(TableUniqueName.uname(vc, tableNameItem), alias);
		return tableNameItem + " " + alias;
	}
	
	@Override
	protected String tableSourceReplacer(String originalTableName) {
		TableUniqueName uTableName = TableUniqueName.uname(vc, originalTableName);
		TableUniqueName newTableSource = vc.getMeta().getSampleTableNameIfExistsElseOriginal(uTableName);
		// note: newTableSource might be same as uTableName if there's no sample table exists.
		if (!uTableName.equals(newTableSource)) {
			replacedTableSources.add(uTableName);
			cumulativeReplacedTableSources.put(uTableName, newTableSource);
		}
		return newTableSource.toString();
	}
	
	@Override
	protected String tableNameReplacer(String originalTableName) {
		if (tableAliases.values().contains(originalTableName)) {
			// we don't have to replace the table name if aliases were used.
			return originalTableName;
		} else {
			// find the name of the effective table (whether an original table or a sample table), then find the 
			// proper alias we used for the table source.
			return tableAliases.get(vc.getMeta().getSampleTableNameIfExistsElseOriginal(
					TableUniqueName.uname(vc, originalTableName))).toString();
		}
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
	
	protected int select_list_elem_num = 0;	// 1 for the first column, 2 for the second column, and so on.
	
	protected String quoteString() {
		return vc.getDbms().getQuoteString();
	}
	
	@Override
	public String visitSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx) {
		select_list_elem_num++;
		String newSelectListElem = null;
		Pair<String, Alias> colName2Alias = null;
		
		if (ctx.getText().equals("*")) {
			// TODO: replace * with all columns in the (joined) source table.
			newSelectListElem = "*";
		} else {
			StringBuilder elem = new StringBuilder();
			
			// We use a baseRewriter to prevent that "COUNT(*)" is rewritten to "COUNT(*) * (1/sample_ratio)"
			SelectStatementBaseRewriter baseRewriter = new SelectStatementBaseRewriter(queryString);
			String tabColName = baseRewriter.visit(ctx.expression());
			String tabName = NameHelpers.tabNameOfColName(tabColName);
			TableUniqueName tabUniqueName = NameHelpers.tabUniqueNameOfColName(vc, tabColName);
			String colName = NameHelpers.colNameOfColName(tabColName);
			
			// if a table name is specified, we change it to its alias name.
			if (tableAliases.containsKey(tabUniqueName)) {
				tabName = tableAliases.get(tabUniqueName).toString();
			}
			
			// if there was derived table(s), we may need to substitute aliased name for the colName.
			for (Map.Entry<String, Map<String, Alias>> e : derivedTableColName2Aliases.entrySet()) {
				String derivedTabName = e.getKey();
				
				if (tabName.length() > 0 && !tabName.equals(derivedTabName)) {
					// this is the case where there are more than one derived tables, and a user specifically referencing
					// a column in one of those derived tables.
					continue;
				}
				if (e.getValue().containsKey(colName)) {
					Alias alias = e.getValue().get(colName);
					if (alias.autoGenerated()) {
						colName = alias.toString();
					}
				}
			}
			
			if (tabName.length() > 0) {
				elem.append(String.format("%s.%s", tabName, colName));
			} else {
				elem.append(colName);
			}
			
			if (ctx.column_alias() != null) {
				Alias alias = new Alias(colName, ctx.column_alias().getText());
				elem.append(String.format(" AS %s", alias));
				colName2Alias = Pair.of(colName, alias);
			} else {
				// We add a pseudo column alias
				Alias alias = Alias.genAlias(depth, colName);
				elem.append(String.format(" AS %s", alias));
				colName2Alias = Pair.of(baseRewriter.visit(ctx.expression()), alias);
			}
			
			newSelectListElem = elem.toString();
		}
		
		colName2Aliases.add(Pair.of(colName2Alias.getKey(), colName2Alias.getValue()));
		return newSelectListElem;
	}
	
	protected boolean withinQuerySpecification = false;
	
	@Override
	public String visitAggregate_windowed_function(VerdictSQLParser.Aggregate_windowed_functionContext ctx) { 
		if (withinQuerySpecification)
			return visitAggregate_function_within_query_specification(ctx);
		else
			return visitAggregate_function_outside_query_specification(ctx);
	}
	
	protected String visitAggregate_function_within_query_specification(VerdictSQLParser.Aggregate_windowed_functionContext ctx) {
		while (aggColumnIndicator.size() < select_list_elem_num-1) {
			aggColumnIndicator.add(false);		// pad zero
		}
		aggColumnIndicator.add(true);
		
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
	
	protected String visitAggregate_function_outside_query_specification(VerdictSQLParser.Aggregate_windowed_functionContext ctx) {
		return super.visitAggregate_windowed_function(ctx);
	}
	
	protected String extraIndentBeforeTableSourceName(int sourceIndex) {
		return (sourceIndex == 1) ? "" : " ";
	}
	
	@Override
	public String visitQuery_specification(VerdictSQLParser.Query_specificationContext ctx) {
		withinQuerySpecification = true;
		// We process the FROM clause first to get the ratio between the samples (if we use them) and the
		// original tables.
		// As we process the FROM clause, we need to get alias information as well.
		String fromClause = rewrittenFromClause(ctx);
		
		String selectList = rewrittenSelectList(ctx);
		
		String whereClause = rewrittenWhereClause(ctx);
		
		String groupbyClause = rewrittenGroupbyClause(ctx);
		
		StringBuilder query = new StringBuilder(1000);
		query.append(indentString); query.append(selectList.toString());
		query.append("\n" + indentString); query.append(fromClause.toString());
		if (whereClause.length() > 0) { query.append("\n"); query.append(indentString); query.append(whereClause.toString()); }
		if (groupbyClause.length() > 0) { query.append("\n"); query.append(indentString); query.append(groupbyClause.toString()); }
		
		withinQuerySpecification = false;
		return query.toString();
	}
	
	protected String rewrittenFromClause(VerdictSQLParser.Query_specificationContext ctx) {
		StringBuilder fromClause = new StringBuilder(200);
		fromClause.append("FROM ");
		int sourceIndex = 1;
		for (VerdictSQLParser.Table_sourceContext tctx : ctx.table_source()) {
			if (sourceIndex == 1) {
				fromClause.append(extraIndentBeforeTableSourceName(sourceIndex) + visit(tctx));
			} else {
				fromClause.append(String.format(",%s%s", extraIndentBeforeTableSourceName(sourceIndex), visit(tctx)));
			}
			sourceIndex++;
		}
		return fromClause.toString();
	}
	
	protected String rewrittenSelectList(VerdictSQLParser.Query_specificationContext ctx) {
		StringBuilder selectList = new StringBuilder(200);
		selectList.append("SELECT ");
		selectList.append(visit(ctx.select_list()));
		return selectList.toString();
	}
	
	protected String rewrittenWhereClause(VerdictSQLParser.Query_specificationContext ctx) {
		StringBuilder whereClause = new StringBuilder(200);
		if (ctx.where != null) {
			whereClause.append("WHERE ");
			whereClause.append(visit(ctx.where));
		}
		return whereClause.toString();
	}
	
	protected String rewrittenGroupbyClause(VerdictSQLParser.Query_specificationContext ctx) {
		StringBuilder otherClause = new StringBuilder(200);
		if (ctx.group_by_item() != null && ctx.group_by_item().size() > 0) {
			otherClause.append("GROUP BY ");
			for (VerdictSQLParser.Group_by_itemContext gctx : ctx.group_by_item()) {
				otherClause.append(visit(gctx));
			}
		}
		return otherClause.toString();
	}
	
	@Override
	public String visitDerived_table_source_item(VerdictSQLParser.Derived_table_source_itemContext ctx) {
		AnalyticSelectStatementRewriter subqueryVisitor = new AnalyticSelectStatementRewriter(vc, queryString);
		subqueryVisitor.setIndentLevel(defaultIndent + 4);
		subqueryVisitor.setDepth(depth+1);
		String derivedTable = subqueryVisitor.visit(ctx.derived_table().subquery().select_statement().query_expression());
		cumulativeReplacedTableSources.putAll(subqueryVisitor.getCumulativeSampleTables());
		
		Alias alias = new Alias("subquery", ctx.as_table_alias().getText());
		derivedTableColName2Aliases.put(alias.toString(), TypeCasting.listToMap(subqueryVisitor.getColName2Aliases()));
		
		return String.format("(\n%s) %s ", derivedTable, alias);
	}
	
//	@Override
//	public String visitSubquery(VerdictSQLParser.SubqueryContext ctx) {
//		AnalyticSelectStatementRewriter subqueryVisitor = new AnalyticSelectStatementRewriter(vc, queryString);
//		subqueryVisitor.setIndentLevel(defaultIndent + 4);
//		subqueryVisitor.setDepth(depth+1);
//		String ret = subqueryVisitor.visit(ctx.select_statement().query_expression());
//		cumulativeReplacedTableSources.putAll(subqueryVisitor.getCumulativeSampleTables());
////		derivedTableColName2Aliases.put()
//		return ret;
//	}
	
	@Override
	public String visitGroup_by_item(VerdictSQLParser.Group_by_itemContext ctx) {
		String groupName = ctx.getText();
		Alias alias = new Alias(groupName, groupName);
		
		for (Pair<String, Alias> e : colName2Aliases) {
			if (NameHelpers.colNameOfColName(e.getKey()).equals(groupName)) {
				alias = e.getValue();
				break;
			}
		}
		
		if (isFirstGroup) {
			isFirstGroup = false;
			return alias.toString();
		}
		else {
			return ", " + alias;
		}
	}
	
}