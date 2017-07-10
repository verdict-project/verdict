package edu.umich.verdict.query;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StringManupulations;

public class UseDatabaseQuery extends Query {

	public UseDatabaseQuery(VerdictContext vc, String q) {
		super(vc, q);
	}

	@Override
	public void compute() throws VerdictException {
		VerdictSQLParser p = StringManupulations.parserOf(queryString);

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
	}
}
