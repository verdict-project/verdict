package edu.umich.verdict.relation.expr;

import edu.umich.verdict.exceptions.VerdictException;

public abstract class ExprVisitor<T> {

	public ExprVisitor() {
	}
	
	public T visit(Expr expr) {
		return expr.accept(this);
	}
	
	public abstract T call(Expr expr);

}
