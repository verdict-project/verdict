package edu.umich.verdict.query;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.dbms.DbmsJDBC;
import edu.umich.verdict.dbms.DbmsSpark;
import edu.umich.verdict.dbms.DbmsSpark2;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.parser.VerdictSQLParser.Table_nameContext;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public class DescribeTableQuery extends SelectQuery {

	public DescribeTableQuery(VerdictContext vc, String q) {
		super(vc, q);
	}

	@Override
	public void compute() throws VerdictException {
		VerdictSQLParser p = StringManipulations.parserOf(queryString);

		VerdictSQLBaseVisitor<TableUniqueName> visitor = new VerdictSQLBaseVisitor<TableUniqueName>() {
			private TableUniqueName tableName;

			protected TableUniqueName defaultResult() { return tableName; }
			
			@Override
			public TableUniqueName visitDescribe_table_statement(VerdictSQLParser.Describe_table_statementContext ctx) {
				String schema = null;
				Table_nameContext t = ctx.table_name();
				if (t.schema != null) {
					schema = t.schema.getText();
				}
				String table = t.table.getText();
				tableName = TableUniqueName.uname(schema, table);
				return tableName;
			}
		};
		
		TableUniqueName tableName = visitor.visit(p.describe_table_statement());
		TableUniqueName table = (tableName.getSchemaName() != null)? tableName : TableUniqueName.uname(vc, tableName.getTableName());
		
		if (table.getSchemaName() == null) {
			VerdictLogger.info("No database schema selected or specified; cannot show tables.");
		} else {
			if (vc.getDbms().isJDBC()) {
				rs = ((DbmsJDBC) vc.getDbms()).describeTableInResultSet(table);
			} else if (vc.getDbms().isSpark()) {
				df = ((DbmsSpark) vc.getDbms()).describeTableInDataFrame(table);
			} else if (vc.getDbms().isSpark2()) {
				ds = ((DbmsSpark2) vc.getDbms()).describeTableInDataFrame(table);
			}
		}
	}
}
