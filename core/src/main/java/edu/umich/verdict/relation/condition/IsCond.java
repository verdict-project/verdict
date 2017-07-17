package edu.umich.verdict.relation.condition;

import edu.umich.verdict.relation.expr.Expr;

public class IsCond extends Cond {

	private Expr left;
	
	private Cond right;
	
	public IsCond(Expr left, Cond right) {
		this.left = left;
		this.right = right;
	}
	
	@Override
	public String toString() {
		return String.format("(%s IS %s)", left, right);
	}
	
	@Override
	public Cond withTableSubstituted(String newTab) {
		return new IsCond(left.withTableSubstituted(newTab), right.withTableSubstituted(newTab));
	}
	
	@Override
	public String toSql() {
		return String.format("(%s IS %s)", left.toSql(), right.toSql());
	}
}
