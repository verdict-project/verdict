package edu.umich.verdict.query;

import java.util.HashMap;
import java.util.Map;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.util.VerdictLogger;

public class BootstrapSelectStatementRewriter extends AnalyticSelectStatementRewriter {
	
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
	
	protected String multiplicityExpression() {
		return multiplicityExpression(null);
	}
	
	protected String multiplicityExpression(String param) {
		if (param != null && param.equals("1")) {
			return "1 AS verdict_mul";
		} else {
			StringBuilder elem = new StringBuilder();
			elem.append("(case");
			for (int k = conditionalProb.length - 1; k > 0 ; k--) {
				elem.append(String.format(" when rand() > %.8f then %d", conditionalProb[k], k));
			}
			elem.append(" else 0 end) AS verdict_mul");
			return elem.toString();
		}
	}
	
	@Override
	protected String extraIndentBeforeTableSourceName(int sourceIndex) {
		return (sourceIndex == 1) ? "" : "\n" + indentString + "      ";
	}
	
	protected TableUniqueName originalTableNameItem = null;
	
	@Override
	public String visitHinted_table_name_item(VerdictSQLParser.Hinted_table_name_itemContext ctx) {
		String tableNameItem = visit(ctx.table_name_with_hint());
		String alias = null;
		if (ctx.as_table_alias() == null) {
			alias = genAlias();
		} else {
			alias = ctx.as_table_alias().getText();
		}
		tableAliases.put(originalTableNameItem, alias);
		return tableNameItem + " " + alias;
	}
	
	@Override
	protected String tableSourceReplacer(String originalTableName) {
		TableUniqueName uTableName = TableUniqueName.uname(vc, originalTableName);
		TableUniqueName newTableSource = vc.getMeta().getSampleTableNameIfExistsElseOriginal(uTableName);
		originalTableNameItem = newTableSource;
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
	
	protected String tableSourceReplacerSingleNested(String originalTableName) {
		TableUniqueName uTableName = TableUniqueName.uname(vc, originalTableName);
		TableUniqueName newTableSource = vc.getMeta().getSampleTableNameIfExistsElseOriginal(uTableName);
		originalTableNameItem = newTableSource;
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
	
}

