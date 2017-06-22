package edu.umich.verdict.relation.expr;

import edu.umich.verdict.relation.Relation;

public abstract class ExprModifier {

	public ExprModifier() {}

	public Expr visit(Expr expr) {
		return expr.accept(this);
	}
	
	public Expr call(Expr expr) {
		return expr;
	}
	
	public Relation call(Relation r) {
		return r;
	}
	
}
