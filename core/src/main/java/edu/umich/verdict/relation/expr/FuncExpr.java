package edu.umich.verdict.relation.expr;

import java.util.Map;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.exceptions.VerdictException;

public class FuncExpr extends Expr {
	
	public enum FuncName {
		COUNT, SUM, AVG, COUNT_DISTINCT, IMPALA_APPROX_COUNT_DISTINCT,
		ROUND, MAX, MIN, FLOOR, CEIL, EXP, LN, LOG10, LOG2, SIN, COS, TAN,
		SIGN, RAND, UNIX_TIMESTAMP,
		UNKNOWN
	}
	
	protected Expr expression;
	
	protected FuncName funcname;
	
	protected static Map<String, FuncName> string2FunctionType = ImmutableMap.<String, FuncName>builder()
			.put("ROUND", FuncName.ROUND)
			.put("FLOOR", FuncName.FLOOR)
			.put("CEIL", FuncName.CEIL)
			.put("EXP", FuncName.EXP)
			.put("LN", FuncName.LN)
			.put("LOG10", FuncName.LOG10)
			.put("LOG2", FuncName.LOG2)
			.put("SIN", FuncName.SIN)
			.put("COS", FuncName.COS)
			.put("TAN", FuncName.TAN)
			.put("SIGN", FuncName.SIGN)
			.put("RAND", FuncName.RAND)
			.put("UNIX_TIMESTAMP", FuncName.UNIX_TIMESTAMP)
			.build();

	protected static Map<FuncName, String> functionPattern = ImmutableMap.<FuncName, String>builder()
			.put(FuncName.COUNT,  "COUNT(%s)")
			.put(FuncName.SUM, "SUM(%s)")
			.put(FuncName.AVG, "AVG(%s)")
			.put(FuncName.COUNT_DISTINCT, "COUNT(DISTINCT %s)")
			.put(FuncName.IMPALA_APPROX_COUNT_DISTINCT, "NDV(%s)")
			.put(FuncName.ROUND, "ROUND(%s)")
			.put(FuncName.MIN, "MIN(%s)")
			.put(FuncName.MAX, "MAX(%s)")
			.put(FuncName.FLOOR, "FLOOR(%s)")
			.put(FuncName.CEIL, "CEIL(%s)")
			.put(FuncName.EXP, "EXP(%s)")
			.put(FuncName.LN, "LN(%s)")
			.put(FuncName.LOG10, "LOG10(%s)")
			.put(FuncName.LOG2, "LOG2(%s)")
			.put(FuncName.SIN, "SIN(%s)")
			.put(FuncName.COS, "COS(%s)")
			.put(FuncName.TAN, "TAN(%s)")
			.put(FuncName.SIGN, "SIGN(%s)")
			.put(FuncName.RAND, "RAND(%s)")
			.put(FuncName.UNIX_TIMESTAMP, "UNIX_TIMESTAMP(%s)")
			.put(FuncName.UNKNOWN, "UNKNOWN(%s)")
			.build();
	
	public FuncExpr(FuncName fname, Expr expr) {
		this.expression = expr;
		this.funcname = fname;
	}
	
	public static FuncExpr from(String expr) {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(expr));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		return from(p.function_call());
	}
	
	public static FuncExpr from(VerdictSQLParser.Function_callContext ctx) {
		VerdictSQLBaseVisitor<FuncExpr> v = new VerdictSQLBaseVisitor<FuncExpr>() {
			@Override
			public FuncExpr visitAggregate_windowed_function(VerdictSQLParser.Aggregate_windowed_functionContext ctx) {
				if (ctx.AVG() != null) {
					return new FuncExpr(FuncName.AVG, Expr.from(ctx.all_distinct_expression().expression()));
				} else if (ctx.SUM() != null) {
					return new FuncExpr(FuncName.SUM, Expr.from(ctx.all_distinct_expression().expression()));
				} else if (ctx.COUNT() != null) {
					if (ctx.all_distinct_expression() != null && ctx.all_distinct_expression().DISTINCT() != null) {
						return new FuncExpr(FuncName.COUNT_DISTINCT, Expr.from(ctx.all_distinct_expression().expression()));
					} else {
						return new FuncExpr(FuncName.COUNT, new StarExpr());
					}
				} else {
					return new FuncExpr(FuncName.UNKNOWN, Expr.from(ctx.all_distinct_expression().expression()));
				}
			}
			
			@Override
			public FuncExpr visitMathematical_function_expression(VerdictSQLParser.Mathematical_function_expressionContext ctx) {
				if (ctx.unary_mathematical_function() != null) {
					String fname = ctx.unary_mathematical_function().getText().toUpperCase();
					if (string2FunctionType.containsKey(fname)) {
						if (ctx.expression() == null) {
							return new FuncExpr(string2FunctionType.get(fname), null);
						} else {
							return new FuncExpr(string2FunctionType.get(fname), Expr.from(ctx.expression()));
						}
					} else {
						return new FuncExpr(FuncName.UNKNOWN, Expr.from(ctx.expression()));
					}
				} else {
					String fname = ctx.noparam_mathematical_function().getText().toUpperCase();
					if (string2FunctionType.containsKey(fname)) {
						return new FuncExpr(string2FunctionType.get(fname), null);
					} else {
						return new FuncExpr(FuncName.UNKNOWN, null);
					}
				}
			}
		};
		return v.visit(ctx);
	}
	
	public FuncName getFuncName() {
		return funcname;
	}
	
	public Expr getExpr() {
		return expression;
	}
	
	public String getExprInString() {
		return getExpr().toString();
	}
	
	public static FuncExpr avg(Expr expr) {
		return new FuncExpr(FuncName.AVG, expr);
	}
	
	public static FuncExpr avg(String expr) {
		return avg(Expr.from(expr));
	}
	
	public static FuncExpr sum(Expr expr) {
		return new FuncExpr(FuncName.SUM, expr);
	}
	
	public static FuncExpr sum(String expr) {
		return sum(Expr.from(expr));
	}
	
	public static FuncExpr count() {
		return new FuncExpr(FuncName.COUNT, new StarExpr());
	}
	
	public static FuncExpr countDistinct(Expr expr) {
		return new FuncExpr(FuncName.COUNT_DISTINCT, expr);
	}
	
	public static FuncExpr countDistinct(String expr) {
		return countDistinct(Expr.from(expr));
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
	
	public static FuncExpr approxCountDistinct(Expr expr, VerdictContext vc) {
		if (vc.getDbms().getName().equalsIgnoreCase("impala")) {
			return new FuncExpr(FuncName.IMPALA_APPROX_COUNT_DISTINCT, expr);
		} else {
			return new FuncExpr(FuncName.COUNT_DISTINCT, expr);
		}
	}
	
	public static FuncExpr approxCountDistinct(String expr, VerdictContext vc) {
		return approxCountDistinct(Expr.from(expr), vc);
	}
	
	@Override
	public String toString() {
		if (expression == null) {
			return String.format(functionPattern.get(funcname), "");
		} else {
			return String.format(functionPattern.get(funcname), expression.toString());
		}
	}

	@Override
	public <T> T accept(ExprVisitor<T> v) {
		return v.call(this);
	}
	
	@Override
	public boolean isagg() {
		if (funcname.equals(FuncName.AVG) || funcname.equals(FuncName.SUM) || funcname.equals(FuncName.COUNT)
			|| funcname.equals(FuncName.COUNT_DISTINCT)) {
			return true;
		} else {
			return false;
		}	
	}
}
