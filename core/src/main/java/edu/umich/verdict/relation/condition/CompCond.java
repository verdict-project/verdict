package edu.umich.verdict.relation.condition;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.ExprModifier;

public class CompCond extends Cond {
	
	private Expr left;
	
	private Expr right;
	
	private String compOp;

	public CompCond(Expr left, Expr right, String compOp ) {
		this.left = left;
		this.right = right;
		this.compOp = compOp;
	}
	
	public static CompCond from(Expr left, Expr right, String compOp) {
		return new CompCond(left, right, compOp);
	}
	
	public Expr getLeft() {
		return left;
	}
	
	public Expr getRight() {
		return right;
	}
	
	public String getOp() {
		return compOp;
	}
	
	@Override
	public Cond accept(CondModifier v) {
		return v.call(this);
	}

	@Override
	public String toString(VerdictContext vc) {
		return String.format("%s %s %s", left, compOp, right);
	}

}
