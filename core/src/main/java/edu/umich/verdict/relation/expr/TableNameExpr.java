package edu.umich.verdict.relation.expr;

// Currently not used.
public class TableNameExpr extends Expr {

	@Override
	public <T> T accept(ExprVisitor<T> v) {
		return v.call(this);
	}

	@Override
	public Expr withTableSubstituted(String newTab) {
		return this;
	}
	
	@Override
	public String toSql() {
		return toString();
	}

}
