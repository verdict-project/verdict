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

	@Override
	public String toString(VerdictContext vc) {
		return String.format("(%s %s %s)", left.toString(vc), op, right.toString(vc));
	}

	public Expr accept(ExprVisitor v) throws VerdictException {
		left = v.visit(left);
		right = v.visit(right);
		return v.call(this);
	}
}
