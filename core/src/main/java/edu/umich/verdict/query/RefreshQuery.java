package edu.umich.verdict.query;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public class RefreshQuery extends Query {

	public RefreshQuery(VerdictContext vc, String q) {
		super(vc, q);
	}

	@Override
	public void compute() throws VerdictException {
		VerdictSQLParser p = StringManipulations.parserOf(queryString);
		VerdictSQLBaseVisitor<String> visitor = new VerdictSQLBaseVisitor<String>() {
			@Override
			public String visitRefresh_statement(VerdictSQLParser.Refresh_statementContext ctx) {
				String schema = null;
				if (ctx.schema != null) {
					schema = ctx.schema.getText();
				}
				return schema;
			}
		};
		String schema = visitor.visit(p.refresh_statement());
		schema = (schema != null)? schema : ( (vc.getCurrentSchema().isPresent())? vc.getCurrentSchema().get() : null );

		if (schema != null) {
			vc.getMeta().refreshSampleInfo(schema);
			vc.getMeta().refreshTables(schema);
		} else {
			String msg = "No schema selected. No refresh done.";
			VerdictLogger.error(msg);
			throw new VerdictException(msg);
		}
	}

}
