package edu.umich.verdict.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.util.StringManupulations;

@Deprecated
public class ApproximateSelectQuery extends SelectQuery {

	public ApproximateSelectQuery(VerdictJDBCContext vc, String queryString) {
		super(vc, queryString);
	}

	/**
	 * Verdict currently supports the aggregation queries when the outer-most query includes aggregations.
	 * TODO: lift this assumption so that we also support the queries with aggregations in subqueries.
	 * @param queryString
	 * @return
	 */
	public static boolean doesSupport(String queryString) {
		VerdictSQLParser p = StringManupulations.parserOf(queryString);
		
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
	
//	@Override
//	public ResultSet compute() throws VerdictException {
//		Pair<String, AnalyticSelectStatementRewriter> rewrittenQueryAndRewriter = rewriteQuery();
//		
//		String rewrittenQuery = rewrittenQueryAndRewriter.getLeft();
//		AnalyticSelectStatementRewriter rewriter = rewrittenQueryAndRewriter.getRight();
////		List<Boolean> AggregateColumnIndicator = rewrittenQueryAndRewriter.getRight().getAggregateColumnIndicator();
//		Map<TableUniqueName, TableUniqueName> replacedTables = rewriter.getCumulativeSampleTables();
//		Map<Integer, Integer> meanAndErrorColumnMap = rewriter.getMean2ErrorColumnMap();
//		
//		VerdictLogger.debug(this, "The input query was rewritten to:");
//		VerdictLogger.debugPretty(this, rewrittenQuery, "  ");
//		
//		for (TableUniqueName original : replacedTables.keySet()) {
//			VerdictLogger.info(String.format("Verdict is using a sample table for %s", original));
//		}
//		
//		ResultSet rsFromDB = vc.getDbms().executeQuery(rewrittenQuery);
//		VerdictApproxResultMeta columnInfo = new VerdictApproxResultMeta(
//				meanAndErrorColumnMap,
//				TypeCasting.listToReverseMap(rewriter.getColName2Aliases()),
//				rsFromDB);
//		
//		ResultSet rs = new VerdictResultSet(rsFromDB, columnInfo);
//		return rs;
//	}
	
	// TODO: this will not be used in the future.
	// The errors must be estimated using the variance.
	private List<Double> convertAggColumnToDefaultError(List<Boolean> AggregateColumnIndicator,
			Map<TableUniqueName, TableUniqueName> replacedTables) {
		List<Double> errors = new ArrayList<Double>();
		for (Boolean ind : AggregateColumnIndicator) {
			errors.add((ind && replacedTables.size() > 0)? 0.05 : 0.0);
		}
		return errors;
	}
	
//	protected Pair<String, AnalyticSelectStatementRewriter> rewriteQuery() throws VerdictException {
//		VerdictSQLParser p = StringManupulations.parserOf(queryString);
//		
//		AnalyticSelectStatementRewriter queryRewriter = null;
//		
//		if (vc.getConf().get("approximation_method").equals("bootstrap")) {
//			queryRewriter = new BootstrapSelectStatementRewriter(vc, queryString);
//		} else {
//			queryRewriter = new AnalyticSelectStatementRewriter(vc, queryString);
//		}
//		
//		String rewrittenQuery = queryRewriter.visit(p.select_statement());
//		if (queryRewriter.getException() != null) {
//			throw queryRewriter.getException();
//		}
//		return Pair.of(rewrittenQuery, queryRewriter);
//	}
	
	// bootstrapping rules
	//
	// Keeping multiplicities
	// 1. as soon as aggregation appears (possibly with a groupby clause), we can drop the multiplicities.
	// 2. otherwise, we need to propagate the multiplicities to the outer query.
	// 3. if the multiplicity remains at the most outer query, we can drop the multiplicities.
	//
	// Keysets
	// 1. If a top-level query includes a groupby; then, the groupby columns should be keysets. (this is always the case).
	// 2. We don't have to track the keysets for the subqueries in the where clause.
};
