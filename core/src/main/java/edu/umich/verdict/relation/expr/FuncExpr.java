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
		SIGN, RAND, UNIX_TIMESTAMP, FNV_HASH, ABS, STDDEV, SQRT,
		UNKNOWN
	}
	
	protected Expr expression;
	
	protected FuncName funcname;
	
	protected OverClause overClause;
	
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
			.put("FNV_HASH", FuncName.FNV_HASH)
			.put("ABS", FuncName.ABS)
			.put("STDDEV", FuncName.STDDEV)
			.put("SQRT", FuncName.SQRT)
			.build();

	protected static Map<FuncName, String> functionPattern = ImmutableMap.<FuncName, String>builder()
			.put(FuncName.COUNT,  "count(%s)")
			.put(FuncName.SUM, "sum(%s)")
			.put(FuncName.AVG, "avg(%s)")
			.put(FuncName.COUNT_DISTINCT, "count(distinct %s)")
			.put(FuncName.IMPALA_APPROX_COUNT_DISTINCT, "ndv(%s)")
			.put(FuncName.ROUND, "round(%s)")
			.put(FuncName.MIN, "min(%s)")
			.put(FuncName.MAX, "max(%s)")
			.put(FuncName.FLOOR, "floor(%s)")
			.put(FuncName.CEIL, "ceil(%s)")
			.put(FuncName.EXP, "exp(%s)")
			.put(FuncName.LN, "ln(%s)")
			.put(FuncName.LOG10, "log10(%s)")
			.put(FuncName.LOG2, "log2(%s)")
			.put(FuncName.SIN, "sin(%s)")
			.put(FuncName.COS, "cos(%s)")
			.put(FuncName.TAN, "tan(%s)")
			.put(FuncName.SIGN, "sign(%s)")
			.put(FuncName.RAND, "rand(%s)")
			.put(FuncName.UNIX_TIMESTAMP, "unix_timestamp(%s)")
			.put(FuncName.FNV_HASH, "fnv_hash(%s)")
			.put(FuncName.ABS, "abs(%s)")
			.put(FuncName.STDDEV, "stddev(%s)")
			.put(FuncName.SQRT, "sqrt(%s)")
			.put(FuncName.UNKNOWN, "UNKNOWN(%s)")
			.build();
	
	public FuncExpr(FuncName fname, Expr expr, OverClause overClause) {
		this.expression = expr;
		this.funcname = fname;
		this.overClause = overClause;
	}
	
	public FuncExpr(FuncName fname, Expr expr) {
		this(fname, expr, null);
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
				FuncName fname;
				Expr expr = null;
				if (ctx.all_distinct_expression() != null) {
					expr = Expr.from(ctx.all_distinct_expression().expression());
				}
				OverClause overClause = null;
				
				if (ctx.AVG() != null) {
					fname = FuncName.AVG;
				} else if (ctx.SUM() != null) {
					fname = FuncName.SUM;
				} else if (ctx.COUNT() != null) {
					if (ctx.all_distinct_expression() != null && ctx.all_distinct_expression().DISTINCT() != null) {
						fname = FuncName.COUNT_DISTINCT;
					} else {
						fname = FuncName.COUNT;
						expr = new StarExpr();
					}
				} else {
					fname = FuncName.UNKNOWN;
				}
				
				if (ctx.over_clause() != null) {
					overClause = OverClause.from(ctx.over_clause());
				}
				
				return new FuncExpr(fname, expr, overClause);
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
	
	public static FuncExpr stddev(Expr expr) {
		return new FuncExpr(FuncName.STDDEV, expr);
	}
	
	public static FuncExpr sqrt(Expr expr) {
		return new FuncExpr(FuncName.SQRT, expr);
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
		StringBuilder sql = new StringBuilder(50);
		if (expression == null) {
			sql.append(String.format(functionPattern.get(funcname), ""));
		} else {
			sql.append(String.format(functionPattern.get(funcname), expression.toString()));
		}
		
		if (overClause != null) {
			sql.append(" " + overClause.toString());
		}
		return sql.toString();
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
		} else if (expression != null) {
			return expression.isagg();
		} else {
			return false;
		}
	}
}
