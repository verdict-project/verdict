package edu.umich.verdict.query;

import java.sql.ResultSet;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.datatypes.VerdictResultSet;
import edu.umich.verdict.exceptions.VerdictException;

public class UseDatabaseQuery extends Query {

	public UseDatabaseQuery(String q, VerdictContext vc) {
		super(q, vc);
	}

	public UseDatabaseQuery(Query parent) {
		super(parent.queryString, parent.vc);
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
		
		vc.getDbms().changeDatabase(visitor.visit(p.use_statement()));
		
		return null;
	}
}
