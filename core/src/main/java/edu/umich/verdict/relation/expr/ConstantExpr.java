package edu.umich.verdict.relation.expr;

import edu.umich.verdict.VerdictContext;

public class ConstantExpr extends Expr {
	
	private Object value;

	public ConstantExpr(VerdictContext vc, Object value) {
	    super(vc);
		this.value = value;
	}
	
	public static ConstantExpr from(VerdictContext vc, Object value) {
		return new ConstantExpr(vc, value);
	}
	
	public static ConstantExpr from(VerdictContext vc, String value) {
		return from(vc, (Object) value); 
	}

	@Override
	public String toString() {
		return value.toString();
	}

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
