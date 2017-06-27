package edu.umich.verdict.relation.expr;

import edu.umich.verdict.relation.Relation;

public class SubqueryExpr extends Expr {
	
	private Relation subquery;
	
	public SubqueryExpr(Relation subquery) {
		this.subquery = subquery;
	}
	
	public static SubqueryExpr from(Relation r) {
		return new SubqueryExpr(r);
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
