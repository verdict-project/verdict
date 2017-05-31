package edu.umich.verdict.query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.datatypes.VerdictResultSet;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

public class VerdictDescribeTableQuery extends VerdictQuery {

	public VerdictDescribeTableQuery(String q, VerdictContext vc) {
		super(q, vc);
		// TODO Auto-generated constructor stub
	}

	public VerdictDescribeTableQuery(VerdictQuery parent) {
		super(parent.queryString, parent.vc);
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
		
		if (tableUniqueName.schemaName == null) {
			VerdictLogger.info("No database schema selected or specified; cannot show tables.");
			return null;
		} else {
			return vc.getDbms().describeTable(tableUniqueName);
		}
	}
}
