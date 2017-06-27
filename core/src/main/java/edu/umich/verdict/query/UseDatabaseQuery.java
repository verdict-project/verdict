package edu.umich.verdict.query;

import java.sql.ResultSet;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.exceptions.VerdictException;

public class UseDatabaseQuery extends Query {

	public UseDatabaseQuery(VerdictContext vc, String q) {
		super(vc, q);
	}

	@Override
	public ResultSet compute() throws VerdictException {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(queryString));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));

		VerdictSQLBaseVisitor<String> visitor = new VerdictSQLBaseVisitor<String>() {
			private String schemaName;

			protected String defaultResult() { return schemaName; }
			
			@Override
			public String visitUse_statement(VerdictSQLParser.Use_statementContext ctx) {
				schemaName = ctx.database.getText();
				return schemaName;
			}
		};
		
		String schema = visitor.visit(p.use_statement());
		vc.getDbms().changeDatabase(schema);
		vc.getMeta().refreshSampleInfo(schema);
		
		return null;
	}
}
