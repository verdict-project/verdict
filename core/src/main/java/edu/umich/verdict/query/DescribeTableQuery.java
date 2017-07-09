package edu.umich.verdict.query;

import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.dbms.DbmsJDBC;
import edu.umich.verdict.dbms.DbmsSpark;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StringManupulations;
import edu.umich.verdict.util.VerdictLogger;

public class DescribeTableQuery extends SelectQuery {

	public DescribeTableQuery(VerdictJDBCContext vc, String q) {
		super(vc, q);
	}

	@Override
	public void compute() throws VerdictException {
		VerdictSQLParser p = StringManupulations.parserOf(queryString);

		VerdictSQLBaseVisitor<String> visitor = new VerdictSQLBaseVisitor<String>() {
			private String tableName;

			protected String defaultResult() { return tableName; }
			
			@Override
			public String visitDescribe_table_statement(VerdictSQLParser.Describe_table_statementContext ctx) {
				tableName = ctx.table.getText();
				return tableName;
			}
		};
		
		String tableName = visitor.visit(p.describe_table_statement());
		TableUniqueName table = TableUniqueName.uname(vc, tableName);
		
		if (table.getSchemaName() == null) {
			VerdictLogger.info("No database schema selected or specified; cannot show tables.");
		} else {
			if (vc.getDbms().isJDBC()) {
				rs = ((DbmsJDBC) vc.getDbms()).describeTableInResultSet(table);
			} else if (vc.getDbms().isSpark()) {
				df = ((DbmsSpark) vc.getDbms()).describeTableInDataFrame(table);
			}
		}
	}
}
