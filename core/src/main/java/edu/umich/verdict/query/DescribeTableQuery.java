package edu.umich.verdict.query;

import java.sql.ResultSet;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

public class DescribeTableQuery extends SelectQuery {

	public DescribeTableQuery(VerdictContext vc, String q) {
		super(vc, q);
	}

	@Override
	public ResultSet compute() throws VerdictException {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(queryString));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));

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
		TableUniqueName tableUniqueName = TableUniqueName.uname(vc, tableName);
		
		if (tableUniqueName.getSchemaName() == null) {
			VerdictLogger.info("No database schema selected or specified; cannot show tables.");
			return null;
		} else {
			return vc.getDbms().describeTable(tableUniqueName);
		}
	}
}
