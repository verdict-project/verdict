package edu.umich.verdict.relation.expr;

import edu.umich.verdict.exceptions.VerdictException;

public abstract class ExprVisitor {

	public ExprVisitor() {}

	public Expr visit(Expr expr) throws VerdictException {
		return expr.accept(this);
	}
	
	public abstract Expr call(Expr expr) throws VerdictException;
	
}
