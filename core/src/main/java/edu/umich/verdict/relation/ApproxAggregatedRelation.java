package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.expr.BinaryOpExpr;
import edu.umich.verdict.relation.expr.ConstantExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.ExprModifier;
import edu.umich.verdict.relation.expr.FuncExpr;

public class ApproxAggregatedRelation extends ApproxRelation {
	
	private ApproxRelation source;
	
	private List<Expr> functions;

	public ApproxAggregatedRelation(VerdictContext vc, ApproxRelation source, List<Expr> functions) {
		super(vc);
		this.source = source;
		this.functions = functions;
	}

	@Override
	public ExactRelation rewrite() {
		List<Expr> scaled = new ArrayList<Expr>();
		for (Expr f : functions) {
			scaled.add(transformForSingleFunction(f));
		}
		return new AggregatedRelation(vc, source.rewrite(), scaled);
	}
	
	@Override
	protected Map<String, String> tableSubstitution() {
		return ImmutableMap.of();
	}
	
	@Override
	protected double samplingProbabilityFor(FuncExpr f) {
		return source.samplingProbabilityFor(f);
	}

	private Expr transformForSingleFunction(Expr f) {
		final Map<String, String> sub = source.tableSubstitution();
		
		ExprModifier v = new ExprModifier() {
			public Expr call(Expr expr) {
				if (expr instanceof FuncExpr) {
					FuncExpr f = (FuncExpr) expr;
					FuncExpr s = (FuncExpr) exprWithTableNamesSubstituted(expr, sub);
					if (f.getFuncName().equals(FuncExpr.FuncName.COUNT) || f.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
						return FuncExpr.round(BinaryOpExpr.from(s, ConstantExpr.from(1.0 / samplingProbabilityFor(f)), "*"));
					} else {
						return BinaryOpExpr.from(s, ConstantExpr.from(1.0 / samplingProbabilityFor(f)), "*");
					}
				} else {
					return expr;
				}
			}
		};
		
		return v.visit(f);
	}
}
