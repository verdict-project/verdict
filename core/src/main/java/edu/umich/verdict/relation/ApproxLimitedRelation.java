package edu.umich.verdict.relation;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;

public class ApproxLimitedRelation extends ApproxRelation {
	
	private ApproxRelation source;
	
	private long limit;
	
	public ApproxLimitedRelation(VerdictContext vc, ApproxRelation source, long limit) {
		super(vc);
		this.source = source;
		this.limit = limit;
		this.alias = source.alias;
	}

	@Override
	public ExactRelation rewriteForPointEstimate() {
		ExactRelation r = new LimitedRelation(vc, source.rewriteForPointEstimate(), limit);
		r.setAliasName(getAliasName());
		return r;
	}
	
	@Override
	public ExactRelation rewriteWithSubsampledErrorBounds() {
		ExactRelation r = new LimitedRelation(vc, source.rewriteWithSubsampledErrorBounds(), limit);
		r.setAliasName(getAliasName());
		return r;
	}
	
	@Override
	public ExactRelation rewriteWithPartition() {
		ExactRelation r = new LimitedRelation(vc, source.rewriteWithPartition(), limit);
		r.setAliasName(getAliasName());
		return r;
	}
	
	@Override
	protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
		return source.samplingProbabilityExprsFor(f);
	}

	@Override
	protected Map<String, String> tableSubstitution() {
		return ImmutableMap.of();
	}

	@Override
	protected String sampleType() {
		return null;
	}

	@Override
	protected List<String> sampleColumns() {
		return null;
	}

}
