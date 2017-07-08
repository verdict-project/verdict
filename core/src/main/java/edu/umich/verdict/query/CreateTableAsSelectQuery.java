package edu.umich.verdict.query;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StringManupulations;
import edu.umich.verdict.util.VerdictLogger;

public class CreateTableAsSelectQuery extends Query {

	public CreateTableAsSelectQuery(VerdictContext vc, String q) {
		super(vc, q);
	}
	
	@Override
	public ResultSet compute() throws VerdictException {
		String rewrittenQuery = rewriteQuery(queryString);
		
		VerdictLogger.debug(this, "The input query was rewritten to:");
		VerdictLogger.debugPretty(this, rewrittenQuery, "  ");
		
		vc.getDbms().executeUpdate(rewrittenQuery);
		return null;
	}

	private String rewriteQuery(final String query) {
		VerdictSQLParser p = StringManupulations.parserOf(queryString);
		
		SelectStatementBaseRewriter visitor = new SelectStatementBaseRewriter(query) {
			@Override
			public String visitCreate_table_as_select(VerdictSQLParser.Create_table_as_selectContext ctx) {
				StringBuilder sql = new StringBuilder();
				sql.append("CREATE TABLE");
				if (ctx.IF() != null) sql.append(" IF NOT EXISTS");
				sql.append(String.format(" %s AS \n", ctx.table_name().getText()));
				AnalyticSelectStatementRewriter selectVisitor = new AnalyticSelectStatementRewriter(vc, query);
				selectVisitor.setIndentLevel(2);
				sql.append(selectVisitor.visit(ctx.select_statement()));
				return sql.toString();
			}
		};
		
		return visitor.visit(p.create_table_as_select());
	}
}
