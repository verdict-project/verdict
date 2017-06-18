package edu.umich.verdict.relation;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;

public class ApproxOrderedRelation extends ApproxRelation {
	
	private ApproxRelation source;
	
	private List<ColNameExpr> orderby;
	
	public ApproxOrderedRelation(VerdictContext vc, ApproxRelation source, List<ColNameExpr> orderby) {
		super(vc);
		this.source = source;
		this.orderby = orderby;
	}

	@Override
	public ExactRelation rewrite() {
		return new OrderedRelation(vc, source.rewrite(), orderby); 
	}

	@Override
	protected double samplingProbabilityFor(FuncExpr f) {
		return source.samplingProbabilityFor(f);
	}

	@Override
	protected Map<String, String> tableSubstitution() {
		return ImmutableMap.of();
	}

}
