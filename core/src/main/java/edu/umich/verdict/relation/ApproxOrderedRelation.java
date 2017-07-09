package edu.umich.verdict.relation;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.OrderByExpr;

public class ApproxOrderedRelation extends ApproxRelation {
	
	private ApproxRelation source;
	
	private List<OrderByExpr> orderby;
	
	public ApproxOrderedRelation(VerdictContext vc, ApproxRelation source, List<OrderByExpr> orderby) {
		super(vc);
		this.source = source;
		this.orderby = orderby;
		this.alias = source.alias;
	}

	@Override
	public ExactRelation rewriteForPointEstimate() {
		ExactRelation r = new OrderedRelation(vc, source.rewriteForPointEstimate(), orderby);
		r.setAliasName(getAliasName());
		return r;
	}
	
	@Override
	public ExactRelation rewriteWithSubsampledErrorBounds() {
		ExactRelation r = new OrderedRelation(vc, source.rewriteWithSubsampledErrorBounds(), orderby);
		r.setAliasName(getAliasName());
		return r;
	}
	
	@Override
	public ExactRelation rewriteWithPartition() {
		ExactRelation r = new OrderedRelation(vc, source.rewriteWithPartition(), orderby);
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
		return source.sampleType();
	}

	@Override
	protected List<String> sampleColumns() {
		return source.sampleColumns();
	}

}
