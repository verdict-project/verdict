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
		return from(p.expression());
	}
	
	public static Expr from(VerdictSQLParser.ExpressionContext ctx) {
		ExpressionGen g = new ExpressionGen();
		return g.visit(ctx);
	}
	
//	public abstract String toString(VerdictContext vc);
	
	@Override
	public String toString() {
		return "Expr Base";
	}
	
	public Expr accept(ExprModifier v) {
		return v.call(this);
	}
	
	public abstract <T> T accept(ExprVisitor<T> v);

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