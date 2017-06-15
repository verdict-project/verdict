package edu.umich.verdict.relation.expr;

import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class FuncExpr extends Expr {
	
	public enum FuncName {
		COUNT, SUM, AVG, COUNT_DISTINCT, ROUND, MAX, MIN
	}
	
	protected Expr expression;
	
	protected FuncName funcname;

	protected Map<FuncName, String> functionPattern = ImmutableMap.<FuncName, String>builder()
			.put(FuncName.COUNT,  "COUNT(%s)")
			.put(FuncName.SUM, "SUM(%s)")
			.put(FuncName.AVG, "AVG(%s)")
			.put(FuncName.COUNT_DISTINCT, "COUNT(DISTINCT %s)")
			.put(FuncName.ROUND, "ROUND(%s)")
			.put(FuncName.MAX, "MAX(%s)")
			.put(FuncName.MIN, "MIN(%s)")
			.build();
	
	public FuncExpr(FuncName fname, Expr expr) {
		this.expression = expr;
		this.funcname = fname;
	}
	
	public FuncName getFuncName() {
		return funcname;
	}
	
	public static FuncExpr avg(Expr expr) {
		return new FuncExpr(FuncName.AVG, expr);
	}
	
	public static FuncExpr avg(String expr) {
		return new FuncExpr(FuncName.AVG, Expr.from(expr));
	}
	
	public static FuncExpr sum(Expr expr) {
		return new FuncExpr(FuncName.SUM, expr);
	}
	
	public static FuncExpr sum(String expr) {
		return new FuncExpr(FuncName.SUM, Expr.from(expr));
	}
	
	public static FuncExpr count() {
		return new FuncExpr(FuncName.COUNT, new StarExpr());
	}
	
	public static FuncExpr countDistinct(Expr expr) {
		return new FuncExpr(FuncName.COUNT_DISTINCT, expr);
	}
	
	public static FuncExpr countDistinct(String expr) {
		return new FuncExpr(FuncName.COUNT_DISTINCT, Expr.from(expr));
	}
	
	public static FuncExpr round(Expr expr) {
		return new FuncExpr(FuncName.ROUND, expr);
	}
	
	public static FuncExpr min(Expr expr) {
		return new FuncExpr(FuncName.MIN, expr);
	}
	
	public static FuncExpr max(Expr expr) {
		return new FuncExpr(FuncName.MAX, expr);
	}
	
	public static FuncExpr approxCountDistinct(Expr expr) {
		return new ApproximateCountDistinctFunction(expr);
	}

	public String toString(VerdictContext vc) {
		return String.format(functionPattern.get(funcname), expression.toString(vc));
	}

	public Expr accept(ExprModifier v) throws VerdictException {
		expression = v.visit(expression);
		return v.call(this);
	}

	@Override
	public <T> T accept(ExprVisitor<T> v) throws VerdictException {
		return v.call(this);
	}
}


class ApproximateCountDistinctFunction extends FuncExpr {

	public ApproximateCountDistinctFunction(Expr expr) {
		super(FuncName.COUNT_DISTINCT, expr);
	}
	
	@Override
	public String toString(VerdictContext vc) {
		if (vc.getDbms().getName().equals("impala")) {
			return String.format("NDV(%s)", expression);
		} else {
			return super.toString(vc);
		}
	}
	
}
