package edu.umich.verdict.relation;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.relation.expr.FuncExpr;

public class ApproxLimitedRelation extends ApproxRelation {
	
	private ApproxRelation source;
	
	private long limit;
	
	public ApproxLimitedRelation(VerdictContext vc, ApproxRelation source, long limit) {
		super(vc);
		this.source = source;
		this.limit = limit;
	}

	@Override
	public ExactRelation rewrite() {
		return new LimitedRelation(vc, source.rewrite(), limit);
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
