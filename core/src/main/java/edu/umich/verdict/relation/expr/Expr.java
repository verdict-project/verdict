package edu.umich.verdict.relation.expr;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

public abstract class Expr {

	public Expr() {}
	
	// TODO: write this using the antlr4 parser.
	public static Expr from(String expr) {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(expr));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		ExpressionGen g = new ExpressionGen();
		return g.visit(p.expression());
	}
	
	public abstract String toString(VerdictContext vc);
	
	@Override
	public String toString() {
		VerdictLogger.error(this, "Calling toString() method without VerdictContext is not allowed.");
		return "toString called without VerdictContext";
	}
	
	public abstract Expr accept(ExprModifier v) throws VerdictException;
	
	public abstract <T> T accept(ExprVisitor<T> v) throws VerdictException;

}

class ExpressionGen extends VerdictSQLBaseVisitor<Expr> {
	@Override
	public Expr visitPrimitive_expression(VerdictSQLParser.Primitive_expressionContext ctx) {
		return ConstantExpr.from(ctx.getText());
	}
	
	@Override
	public Expr visitColumn_ref_expression(VerdictSQLParser.Column_ref_expressionContext ctx) {
		return ColNameExpr.from(ctx.getText());
	}
	
	@Override
	public Expr visitBinary_operator_expression(VerdictSQLParser.Binary_operator_expressionContext ctx) {
		return new BinaryOpExpr(visit(ctx.expression(0)), visit(ctx.expression(1)), ctx.op.getText());  
	}
}