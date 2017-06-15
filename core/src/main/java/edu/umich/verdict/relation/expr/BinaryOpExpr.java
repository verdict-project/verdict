package edu.umich.verdict.relation.expr;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class BinaryOpExpr extends Expr {
	
	private Expr left;
	
	private Expr right;
	
	private String op;

	public BinaryOpExpr(Expr left, Expr right, String op) {
		this.left = left;
		this.right = right;
		this.op = op;
	}
	
	public static BinaryOpExpr from(Expr left, Expr right, String op) {
		return new BinaryOpExpr(left, right, op);
	}

	@Override
	public String toString(VerdictContext vc) {
		return String.format("%s %s %s", left.toString(vc), op, right.toString(vc));
	}

	public Expr accept(ExprModifier v) throws VerdictException {
		left = v.visit(left);
		right = v.visit(right);
		return v.call(this);
	}

	@Override
	public <T> T accept(ExprVisitor<T> v) throws VerdictException {
		return v.call(this);
	}
}
