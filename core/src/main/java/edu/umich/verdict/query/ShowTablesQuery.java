package edu.umich.verdict.query;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.dbms.DbmsJDBC;
import edu.umich.verdict.dbms.DbmsSpark;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public class ShowTablesQuery extends SelectQuery {

	public ShowTablesQuery(VerdictContext vc, String q) {
		super(vc, q);
	}
	
	@Override
	public void compute() throws VerdictException {
		VerdictSQLParser p = StringManipulations.parserOf(queryString);
		VerdictSQLBaseVisitor<String> visitor = new VerdictSQLBaseVisitor<String>() {
			private String schemaName = null;

			protected String defaultResult() { return schemaName; }
			
			@Override public String visitShow_tables_statement(VerdictSQLParser.Show_tables_statementContext ctx) {
				if (ctx.schema != null) {
					schemaName = ctx.schema.getText();
				}
				return schemaName;
			}
		};
		String schema =  visitor.visit(p.show_tables_statement());
		schema = (schema != null)? schema : ( (vc.getCurrentSchema().isPresent())? vc.getCurrentSchema().get() : null );
		
		if (schema == null) {
			VerdictLogger.info("No schema specified; cannot show tables.");
			return;
		} else {
			if (vc.getDbms().isJDBC()) {
				rs = ((DbmsJDBC) vc.getDbms()).getTablesInResultSet(schema);
			} else if (vc.getDbms().isSpark()) {
				df = ((DbmsSpark) vc.getDbms()).getTablesInDataFrame(schema);
			}
		}
	}

}
