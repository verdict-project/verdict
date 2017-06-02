package edu.umich.verdict.query;

import java.util.HashMap;
import java.util.Map;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.util.VerdictLogger;

public class VerdictBootstrappingSelectStatementVisitor extends VerdictApproximateSelectStatementVisitor {
	
	protected Map<TableUniqueName, String> sampleTableAlias = new HashMap<TableUniqueName, String>();

	public VerdictBootstrappingSelectStatementVisitor(VerdictContext vc, String queryString) {
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
		return String.format(
				"(case when rand() > 0.9 then 5"
				+ " when rand() > 0.9 then 4"
				+ " when rand() > 0.9 then 3"
				+ " when rand() > 0.9 then 2"
				+ " when rand() > 0.9 then 1"
				+ " else 0 end)");
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
	
}

