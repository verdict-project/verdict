package edu.umich.verdict.relation.expr;

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
	public String toString() {
		return String.format("(%s %s %s)", left.toString(), op, right.toString());
	}

	@Override
	public <T> T accept(ExprVisitor<T> v) {
		return v.call(this);
	}
	
	@Override
	public boolean isagg() {
		return left.isagg() || right.isagg();
	}
	
	@Override
	public Expr withTableSubstituted(String newTab) {
		return new BinaryOpExpr(left.withTableSubstituted(newTab), right.withTableSubstituted(newTab), op);
	}

	@Override
	public String toSql() {
		return String.format("(%s %s %s)", left.toSql(), op, right.toSql());
	}
}
