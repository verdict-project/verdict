package edu.umich.verdict.relation.expr;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class FuncExpr extends Expr {
	
	public enum FuncName {
		COUNT, SUM, AVG, COUNT_DISTINCT
	}
	
	protected Expr expression;
	
	protected FuncName funcname;

	protected Map<FuncName, String> functionPattern = ImmutableMap.of(
			FuncName.COUNT,  "COUNT(%s)",
			FuncName.SUM, "SUM(%s)",
			FuncName.AVG, "AVG(%s)",
			FuncName.COUNT_DISTINCT, "COUNT(DISTINCT %s)"
			);
	
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
	
	public static FuncExpr sum(Expr expr) {
		return new FuncExpr(FuncName.SUM, expr);
	}
	
	public static FuncExpr count() {
		return new FuncExpr(FuncName.COUNT, new StarExpr());
	}
	
	public static FuncExpr countDistinct(Expr expr) {
		return new FuncExpr(FuncName.COUNT_DISTINCT, expr);
	}
	
	public static FuncExpr approxCountDistinct(Expr expr) {
		return new ApproximateCountDistinctFunction(expr);
	}

	public String toString(VerdictContext vc) {
		return String.format(functionPattern.get(funcname), expression.toString(vc));
	}

	public Expr accept(ExprVisitor v) throws VerdictException {
		expression = v.visit(expression);
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
