package edu.umich.verdict.query;

import java.sql.ResultSet;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

public class VerdictCreateTableAsSelectQuery extends VerdictQuery {

	public VerdictCreateTableAsSelectQuery(String q, VerdictContext vc) {
		super(q, vc);
	}
	
	public VerdictCreateTableAsSelectQuery(VerdictQuery parent) {
		super(parent.queryString, parent.vc);
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
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(query));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		
		VerdictSelectStatementBaseVisitor visitor = new VerdictSelectStatementBaseVisitor(query) {
			@Override
			public String visitCreate_table_as_select(VerdictSQLParser.Create_table_as_selectContext ctx) {
				StringBuilder sql = new StringBuilder();
				sql.append("CREATE TABLE");
				if (ctx.IF() != null) sql.append(" IF NOT EXISTS");
				sql.append(String.format(" %s AS \n", ctx.table_name().getText()));
				VerdictApproximateSelectStatementVisitor selectVisitor = new VerdictApproximateSelectStatementVisitor(vc, query);
				selectVisitor.setIndentLevel(2);
				sql.append(selectVisitor.visit(ctx.select_statement()));
				return sql.toString();
			}
		};
		
		return visitor.visit(p.create_table_as_select());
	}
}
