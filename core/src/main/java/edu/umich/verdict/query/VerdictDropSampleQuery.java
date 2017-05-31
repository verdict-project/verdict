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

public class VerdictDropSampleQuery extends VerdictQuery {

	public VerdictDropSampleQuery(String q, VerdictContext vc) {
		super(q, vc);
	}
	
	public VerdictDropSampleQuery(VerdictQuery parent) {
		super(parent.queryString, parent.vc);
	}
	
	@Override
	public ResultSet compute() throws VerdictException {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(queryString));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		
		VerdictSQLBaseVisitor<String> visitor = new VerdictSQLBaseVisitor<String>() {			
			public String visitDelete_sample_statement(VerdictSQLParser.Delete_sample_statementContext ctx) {
				return ctx.table_name().getText();
			}
		};
		
		String originalTableName = visitor.visit(p.delete_sample_statement());
		vc.getDbms().dropTable(vc.getMeta().sampleTableUniqueNameOf(originalTableName));
		vc.getMeta().deleteSampleInfo(TableUniqueName.uname(vc, originalTableName));
		return null;
	}

}
