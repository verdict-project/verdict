package edu.umich.verdict.relation.expr;

public abstract class ExprModifier {

	public ExprModifier() {}

	public Expr visit(Expr expr) {
		return expr.accept(this);
	}
	
	public abstract Expr call(Expr expr);
	
}
