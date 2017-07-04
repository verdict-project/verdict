package edu.umich.verdict.relation.expr;

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.VerdictSQLParser.ExpressionContext;
import edu.umich.verdict.VerdictSQLParser.Search_conditionContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.util.VerdictLogger;

public abstract class Expr {
	
	private static VerdictContext dummyContext;
	
	static {
		VerdictConf conf = new VerdictConf();
		conf.setDbms("dummy");
		try {
			dummyContext = new VerdictContext(conf);
		} catch (VerdictException e) {
			e.printStackTrace();
		}
	}
	
	public static Expr from(String expr) {
		return from(dummyContext, expr);
	}
	
	public static Expr from(VerdictContext vc, String expr) {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(expr));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		return from(vc, p.expression());
	}
	
	public static Expr from(VerdictSQLParser.ExpressionContext ctx) {
		return from(dummyContext, ctx);
	}
	
	public static Expr from(VerdictContext vc, VerdictSQLParser.ExpressionContext ctx) {
		ExpressionGen g = new ExpressionGen(vc);
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
	
	public boolean isagg() {
		return false;
	}

}

class ExpressionGen extends VerdictSQLBaseVisitor<Expr> {
	
	private VerdictContext vc;
	
	public ExpressionGen(VerdictContext vc) {
		this.vc = vc;
	}
	
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
	
	@Override
	public Expr visitFunction_call_expression(VerdictSQLParser.Function_call_expressionContext ctx) {
		return FuncExpr.from(ctx.function_call());
	}
	
	@Override
	public Expr visitCase_expr(VerdictSQLParser.Case_exprContext ctx) {
		return CaseExpr.from(ctx);
	}
	
	@Override
	public Expr visitBracket_expression(VerdictSQLParser.Bracket_expressionContext ctx) {
		return visit(ctx.expression());
	}
	
	@Override
	public Expr visitSubquery_expression(VerdictSQLParser.Subquery_expressionContext ctx) {
		return SubqueryExpr.from(vc, ctx);
	}
	
}
