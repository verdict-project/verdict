package edu.umich.verdict.relation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.util.VerdictLogger;

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
	protected double samplingProbabilityFor(FuncExpr f) {
		return source.samplingProbabilityFor(f);
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
