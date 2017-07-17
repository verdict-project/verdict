package edu.umich.verdict.query;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.ApproxRelation;
import edu.umich.verdict.relation.ExactRelation;
import edu.umich.verdict.util.StringManipulations;

public class SelectQuery extends Query {
	
	
	public SelectQuery(VerdictContext vc, String queryString) {
		super(vc, queryString);
	}

	@Override
	public void compute() throws VerdictException {
		super.compute();
		ExactRelation r = ExactRelation.from(vc, queryString);
		
		VerdictSQLParser p = StringManipulations.parserOf(queryString);
		VerdictSQLBaseVisitor<Boolean> visitor = new VerdictSQLBaseVisitor<Boolean>() {
			@Override
			public Boolean visitSelect_statement(VerdictSQLParser.Select_statementContext ctx) {
				return (ctx.EXACT() != null)? true : false;
			}
		};
		Boolean exact = visitor.visit(p.select_statement());
		
		if (exact) {
			if (vc.getDbms().isJDBC()) {
				rs = r.collectResultSet();
			} else if (vc.getDbms().isSpark()) {
				df = r.collectDataFrame();
			}
		} else {
			ApproxRelation a = r.approx();
			if (vc.getDbms().isJDBC()) {
				rs = a.collectResultSet();
			} else if (vc.getDbms().isSpark()) {
				df = a.collectDataFrame();
			}
		}
	}
	
}
