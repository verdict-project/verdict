package edu.umich.verdict.relation.expr;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

public abstract class Expr {

	public Expr() {}
	
	// TODO: write this using the antlr4 parser.
	public static Expr from(String expr) { return null; }
	
	public abstract String toString(VerdictContext vc);
	
	@Override
	public String toString() {
		VerdictLogger.error(this, "Calling toString() method without VerdictContext is not allowed.");
		return "toString called without VerdictContext";
	}
	
	public abstract Expr accept(ExprVisitor v) throws VerdictException;

}
