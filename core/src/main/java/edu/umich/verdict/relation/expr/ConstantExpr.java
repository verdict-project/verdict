package edu.umich.verdict.relation.expr;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class ConstantExpr extends Expr {
	
	private Object value;

	public ConstantExpr(Object value) {
		this.value = value;
	}
	
	public static ConstantExpr from(Object value) {
		return new ConstantExpr(value);
	}

	@Override
	public String toString(VerdictContext vc) {
		return value.toString();
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
