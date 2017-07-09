package edu.umich.verdict.relation.expr;

import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.relation.ExactRelation;
import edu.umich.verdict.relation.Relation;

public class SubqueryExpr extends Expr {
	
	private Relation subquery;
	
	public SubqueryExpr(Relation subquery) {
		this.subquery = subquery;
	}
	
	public static SubqueryExpr from(Relation r) {
		return new SubqueryExpr(r);
	}
	
	public static SubqueryExpr from(VerdictJDBCContext vc, VerdictSQLParser.Subquery_expressionContext ctx) {
		return from(ExactRelation.from(vc, ctx.subquery().select_statement()));
	}
	
	public Relation getSubquery() {
		return subquery;
	}

	@Override
	public <T> T accept(ExprVisitor<T> v) {
		return v.call(null);
	}
	
	@Override
	public String toString() {
		return "(" + subquery.toSql() + ")";
	}

}
