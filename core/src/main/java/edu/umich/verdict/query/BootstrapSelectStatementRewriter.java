package edu.umich.verdict.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

public class BootstrapSelectStatementRewriter extends AnalyticSelectStatementRewriter {
	
	final String RAND_COLNAME = vc.getConf().get("bootstrap_random_value_colname");

	final String MULTIPLICITY = vc.getConf().get("bootstrap_multiplicity_colname");

	protected String resampleMethod;
	
	// poissonDist[k] is the probability that a tuple appears k times when n is very large.
	final private static double[] poissonDist = {
			0.367879441171442,
			0.367879441171442,
			0.183939720585721,
			0.061313240195240,
			0.015328310048810,
			0.003065662009762,
			0.000510943668294,
			0.000072991952613,
			0.000009123994077,
			0.000001013777120,
			0.000000101377712
	};
	
	// cumulProb[k] is the probability that a tuple appears k times or less.
	final private static double[] cumulProb;
	static {
		cumulProb = new double[poissonDist.length];
		cumulProb[0] = poissonDist[0];
		for (int i = 1; i < poissonDist.length; i++) {
			cumulProb[i] = cumulProb[i-1] + poissonDist[i];
		}
		cumulProb[poissonDist.length-1] = 1.0;
	}
	
	// conditionalProb[k] is the probability that a tuple appears k times conditioned that the tuple does not appear
	// more than k times.
	final private static double[] conditionalProb;
	
	static {
		conditionalProb = new double[poissonDist.length];
		double cdf = 0;
		for (int i = 0; i < poissonDist.length; i++) {
			cdf += poissonDist[i];
			conditionalProb[i] = poissonDist[i] / cdf;
		}
	}
	
	protected Map<TableUniqueName, String> sampleTableAlias = new HashMap<TableUniqueName, String>();

	public BootstrapSelectStatementRewriter(VerdictContext vc, String queryString) {
		super(vc, queryString);
		resampleMethod = "con";
	}
	
	public void setResampleMethod(String m) throws VerdictException {
		if (m.equals("con") || m.equals("1")) {
			throw new VerdictException("Unexpected resample method: " + m);
		}
		resampleMethod = m;
	}
	
	
	/**
	 * This function is pretty convoluted to make use of the overloaded functions in this class definition and 
	 * {@link AnalyticSelectStatementRewriter#visitQuery_specification(edu.umich.verdict.VerdictSQLParser.Query_specificationContext) 
	 * visitQuery_specification} of the base class for every bootstrap trial.
	 * For calling the visitQuery_specification function, this class defines another function {@link
	 * BootstrapSelectStatementRewriter#visitQuery_specificationForSingleTrial(VerdictSQLParser.Query_specificationContext ctx)
	 * visitQuery_specificationForSingleTrial}.
	 */
	@Override
	public String visitQuery_specification(VerdictSQLParser.Query_specificationContext ctx) {
		StringBuilder sql = new StringBuilder(2000);
		
		// this statement computes the mean value
		AnalyticSelectStatementRewriter meanRewriter = new AnalyticSelectStatementRewriter(vc, queryString);
		meanRewriter.setDepth(depth+1);
		meanRewriter.setIndentLevel(defaultIndent + 6);
		String mainSql = meanRewriter.visit(ctx);
		cumulativeReplacedTableSources.putAll(meanRewriter.getCumulativeSampleTables());
		
		// this statement computes the standard deviation
		BootstrapSelectStatementRewriter varianceRewriter = new BootstrapSelectStatementRewriter(vc, queryString);
		varianceRewriter.setDepth(depth+1);
		varianceRewriter.setIndentLevel(defaultIndent + 6);
		String subSql = varianceRewriter.varianceComputationStatement(ctx);
		
		String leftAlias = genAlias();
		String rightAlias = genAlias();
		
		// we combine those two statements using join.
		List<Pair<String, String>> thisColumnName2Aliases = new ArrayList<Pair<String, String>>();
		
		List<Pair<String, String>> leftColName2Aliases = meanRewriter.getColName2Aliases();
//		List<Boolean> leftAggColIndicator = meanRewriter.getAggregateColumnIndicator();
		
		List<Pair<String, String>> rightColName2Aliases = varianceRewriter.getColName2Aliases();
//		List<Boolean> rightAggColIndicator = varianceRewriter.getAggregateColumnIndicator();
		
		sql.append(String.format("%sSELECT", indentString));
		int leftSelectElemIndex = 0;
		int totalSelectElemIndex = 0;
		for (Pair<String, String> colName2Alias : leftColName2Aliases) {
			leftSelectElemIndex++;
			if (leftSelectElemIndex == 1) sql.append(" ");
			else sql.append(", ");
			
			if (meanRewriter.isAggregateColumn(leftSelectElemIndex)) {
				// mean
				totalSelectElemIndex++;
				String alias = genAlias();
				sql.append(String.format("%s.%s AS %s", leftAlias, colName2Alias.getRight(), alias));
				thisColumnName2Aliases.add(Pair.of(colName2Alias.getLeft(), alias));
				
				// error (standard deviation * 1.96 (for 95% confidence interval))
				totalSelectElemIndex++;
				alias = genAlias();
				String matchingAliasName = null;
				for (Pair<String, String> r : rightColName2Aliases) {
					if (colName2Alias.getLeft().equals(r.getLeft())) {
						matchingAliasName = r.getRight();
					}
				}
				sql.append(String.format(", %s.%s AS %s", rightAlias, matchingAliasName, alias));
				thisColumnName2Aliases.add(Pair.of(colName2Alias.getLeft(), alias));
				
				meanColIndex2ErrColIndex.put(totalSelectElemIndex-1, totalSelectElemIndex);
			} else {
				totalSelectElemIndex++;
				sql.append(String.format("%s.%s AS %s", leftAlias, colName2Alias.getRight(), colName2Alias.getRight()));
				thisColumnName2Aliases.add(Pair.of(colName2Alias.getLeft(), colName2Alias.getRight()));
			}
		}
		colName2Aliases = thisColumnName2Aliases;
		
		sql.append(String.format("\n%sFROM (\n", indentString));
		sql.append(mainSql);
		sql.append(String.format("\n%s     ) AS %s", indentString, leftAlias));
		sql.append(" LEFT JOIN (\n");
		sql.append(subSql);
		sql.append(String.format("%s) AS %s", indentString, rightAlias));
		sql.append(String.format(" ON %s.l_shipmode = %s.l_shipmode", leftAlias, rightAlias));
		
		return sql.toString();
	}
	
	protected String visitQuery_specificationForSingleTrial(VerdictSQLParser.Query_specificationContext ctx) {
		return super.visitQuery_specification(ctx);
	}
	
	protected String varianceFunction() {
		return vc.getDbms().varianceFunction();
	}
	
	protected String stddevFunction() {
		return vc.getDbms().stddevFunction();
	}
	
	protected String varianceComputationStatement(VerdictSQLParser.Query_specificationContext ctx) {
		List<Pair<String, String>> subqueryColName2Aliases = null;
		BootstrapSelectStatementRewriter singleRewriter = null;
		
		StringBuilder unionedFrom = new StringBuilder(2000);
		int trialNum = vc.getConf().getInt("bootstrap_trial_num");
		for (int i = 0; i < trialNum; i++) {
			singleRewriter = new BootstrapSelectStatementRewriter(vc, queryString);
			singleRewriter.setIndentLevel(defaultIndent + 6);
			singleRewriter.setDepth(depth+1);
			String singleTrialQuery = singleRewriter.visitQuery_specificationForSingleTrial(ctx);
			if (i == 0) {
				subqueryColName2Aliases = singleRewriter.getColName2Aliases();
			}
			if (i > 0) unionedFrom.append(String.format("\n%s    UNION\n", indentString));
			unionedFrom.append(singleTrialQuery);
		}
		
		StringBuilder sql = new StringBuilder(2000);
		sql.append(String.format("%sSELECT", indentString));
		int selectElemIndex = 0;
		for (Pair<String, String> e : subqueryColName2Aliases) {
			selectElemIndex++;
			sql.append((selectElemIndex > 1)? ", " : " ");
			if (singleRewriter.isAggregateColumn(selectElemIndex)) {
				String alias = genAlias();
				sql.append(String.format("%s(%s) AS %s",
						stddevFunction(), e.getRight(), alias));
				colName2Aliases.add(Pair.of(e.getLeft(), alias));
			} else {
				if (e.getLeft().equals(e.getRight())) sql.append(e.getLeft());
				else sql.append(String.format("%s AS %s", e.getLeft(), e.getRight()));
				colName2Aliases.add(Pair.of(e.getLeft(), e.getRight()));
			}
		}
		sql.append(String.format("\n%sFROM (\n", indentString));
		sql.append(unionedFrom.toString());
		sql.append(String.format("\n%s) AS %s", indentString, genAlias()));
		sql.append(String.format("\n%sGROUP BY", indentString));
		for (int colIndex = 1; colIndex <= subqueryColName2Aliases.size(); colIndex++) {
			if (!singleRewriter.isAggregateColumn(colIndex)) {
				if (colIndex > 1) {
					sql.append(String.format(", %s", subqueryColName2Aliases.get(colIndex-1).getRight()));
				} else {
					sql.append(String.format(" %s", subqueryColName2Aliases.get(colIndex-1).getRight()));
				}
			}
		}
		
		return sql.toString();
	}
	
	
	@Override
	public String visitAggregate_function_within_query_specification(VerdictSQLParser.Aggregate_windowed_functionContext ctx) {
		while (aggColumnIndicator.size() < select_list_elem_num-1) {
			aggColumnIndicator.add(false);		// pad zero
		}
		aggColumnIndicator.add(true);			// TODO this must be adapted according to the sample size.
		
		if (ctx.AVG() != null) {
			return String.format("AVG((%s) * %s)",
					visit(ctx.all_distinct_expression()),
					MULTIPLICITY,
					getSampleSizeToOriginalTableSizeRatio());
		} else if (ctx.SUM() != null) {
			return String.format("(SUM((%s) * %s) * %f)",
					visit(ctx.all_distinct_expression()),
					MULTIPLICITY,
					getSampleSizeToOriginalTableSizeRatio());
		} else if (ctx.COUNT() != null) {
			return String.format("ROUND((SUM(%s) * %f))", MULTIPLICITY, getSampleSizeToOriginalTableSizeRatio());
		}
		VerdictLogger.error(this, String.format("Unexpected aggregate function expression: %s", ctx.getText()));
		return null;	// we don't handle other aggregate functions for now.
	}
	
	protected String multiplicityExpression() {
		return multiplicityExpression(resampleMethod);
	}
	
	protected String multiplicityExpression(String param) {
		if (param != null && param.equals("1")) {
			return String.format("1 AS %s", MULTIPLICITY);
		} else if (param != null && param.equals("con")) {
			StringBuilder elem = new StringBuilder();
			elem.append("(case");
			for (int k = conditionalProb.length - 1; k > 0 ; k--) {
				elem.append(String.format(" WHEN rand() <= %.10f THEN %d", conditionalProb[k], k));
			}
			elem.append(String.format(" ELSE 0 END) AS %s", MULTIPLICITY));
			return elem.toString();
		} else {
			StringBuilder elem = new StringBuilder();
			elem.append("(case");
			for (int k = 0; k < cumulProb.length-1; k++) {
				elem.append(String.format(" WHEN %s <= %.10f THEN %d", RAND_COLNAME, cumulProb[k], k));
			}
			elem.append(String.format(" ELSE %d END) AS %s", cumulProb.length-1, MULTIPLICITY));
			return elem.toString();
		}
	}
	
	@Override
	protected String extraIndentBeforeTableSourceName(int sourceIndex) {
		return (sourceIndex == 1) ? "" : "\n" + indentString + "      ";
	}
	
	protected TableUniqueName aliasedTableNameItem = null;
	
	@Override
	public String visitHinted_table_name_item(VerdictSQLParser.Hinted_table_name_itemContext ctx) {
		String tableNameItem = visit(ctx.table_name_with_hint());
		String alias = null;
		if (ctx.as_table_alias() == null) {
			alias = genAlias();
		} else {
			alias = ctx.as_table_alias().getText();
		}
		tableAliases.put(aliasedTableNameItem, alias);
		return tableNameItem + " " + alias;
	}
	
	@Override
	protected String tableSourceReplacer(String originalTableName) {
		String bootstrap_sampling_method = vc.getConf().get("bootstrap_sampling_method");
		
		if (bootstrap_sampling_method == null) {
			return tableSourceReplacerSingleNested(originalTableName);	// by default
		} else if (bootstrap_sampling_method.equals("single_nested")) {
			return tableSourceReplacerSingleNested(originalTableName);
		} else if (bootstrap_sampling_method.equals("double_nested")) {
			VerdictLogger.warn(this, "Double-nested bootstrap sampling is erroroneous; thus, it should be avoided.");
			return tableSourceReplacerDoubleNested(originalTableName);
		} else {
			return tableSourceReplacerSingleNested(originalTableName);
		}
	}
	
	protected String tableSourceReplacerSingleNested(String originalTableName) {
		TableUniqueName uTableName = TableUniqueName.uname(vc, originalTableName);
		TableUniqueName newTableSource = vc.getMeta().getSampleTableNameIfExistsElseOriginal(uTableName);
		aliasedTableNameItem = newTableSource;
		if (!uTableName.equals(newTableSource)) {
			replacedTableSources.add(uTableName);
			cumulativeReplacedTableSources.put(uTableName, newTableSource);
			return String.format("(SELECT *, %s\n%sFROM %s)",
					multiplicityExpression(),
					indentString + "      ",
					newTableSource);
		} else {
			return newTableSource.toString();
		}
	}
	
	/**
	 * This must not be used. The rand() AS verdict_rand is not materialized; thus, every call of verdict_rand produces
	 * a different value.
	 * @param originalTableName
	 * @return
	 */
	protected String tableSourceReplacerDoubleNested(String originalTableName) {
		TableUniqueName uTableName = TableUniqueName.uname(vc, originalTableName);
		TableUniqueName newTableSource = vc.getMeta().getSampleTableNameIfExistsElseOriginal(uTableName);
		aliasedTableNameItem = newTableSource;
		if (!uTableName.equals(newTableSource)) {
			replacedTableSources.add(uTableName);
			cumulativeReplacedTableSources.put(uTableName, newTableSource);
			return String.format("(SELECT *, %s\n%sFROM %s",
					multiplicityExpression("cumul"),
					indentString + "      ",
					String.format("(SELECT *, rand() AS %s\n%sFROM %s) %s)",
							RAND_COLNAME,
							indentString + "            ",
							newTableSource,
							genAlias()));
		} else {
			return newTableSource.toString();
		}
	}
	
	@Override
	public String visitOrder_by_clause(VerdictSQLParser.Order_by_clauseContext ctx) {
		StringBuilder orderby = new StringBuilder();
		orderby.append("ORDER BY");
		boolean isFirst = true;
		for (VerdictSQLParser.Order_by_expressionContext octx : ctx.order_by_expression()) {
			String alias = findAliasedExpression(visit(octx.expression()));
			
			if (isFirst) orderby.append(" ");
			else 	     orderby.append(", ");
			
			orderby.append(alias);
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
	
	/**
	 * @param colExpr The original text
	 * @return An alias we declared
	 */
	private String findAliasedExpression(String colExpr) {
		for (Pair<String, String> e : colName2Aliases) {
			if (colExpr.equalsIgnoreCase(e.getLeft())) {
				return e.getRight();
			}
		}
		return null;
	}
	
}

