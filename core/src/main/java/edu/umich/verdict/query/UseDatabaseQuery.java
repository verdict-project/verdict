package edu.umich.verdict.query;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.util.StringManipulations;

public class UseDatabaseQuery extends Query {

	public UseDatabaseQuery(VerdictContext vc, String q) {
		super(vc, q);
	}

	@Override
	public void compute() throws VerdictException {
		VerdictSQLParser p = StringManipulations.parserOf(queryString);

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
		vc.getMeta().refreshSampleInfoIfNeeded(schema);
		vc.getMeta().refreshTables(schema);
	}
}
