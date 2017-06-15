package edu.umich.verdict.relation.expr;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class StarExpr extends Expr {

	public StarExpr() {}

	@Override
	public String toString(VerdictContext vc) {
		return "*";
	}

	@Override
	public Expr accept(ExprModifier v) throws VerdictException {
		return v.call(this);
	}

	@Override
	public <T> T accept(ExprVisitor<T> v) throws VerdictException {
		return v.call(this);
	}

}
