package edu.umich.verdict.relation;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.relation.expr.Expr;
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
		r.setAlias(getAlias());
		return r;
	}
	
	@Override
	public ExactRelation rewriteWithSubsampledErrorBounds() {
		ExactRelation r = new LimitedRelation(vc, source.rewriteWithSubsampledErrorBounds(), limit);
		r.setAlias(getAlias());
		return r;
	}
	
	@Override
	public ExactRelation rewriteWithPartition() {
		ExactRelation r = new LimitedRelation(vc, source.rewriteWithPartition(), limit);
		r.setAlias(getAlias());
		return r;
	}
	
	@Override
	protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
		return source.samplingProbabilityExprsFor(f);
	}

	@Override
	protected Map<TableUniqueName, String> tableSubstitution() {
		return ImmutableMap.of();
	}

	@Override
	public String sampleType() {
		return null;
	}
	
	@Override
	public double cost() {
		return source.cost();
	}

	@Override
	protected List<String> sampleColumns() {
		return null;
	}

	@Override
	protected String toStringWithIndent(String indent) {
		StringBuilder s = new StringBuilder(1000);
		s.append(indent);
		s.append(String.format("%s(%s) [%d]\n", this.getClass().getSimpleName(), getAlias(), limit));
		s.append(source.toStringWithIndent(indent + "  "));
		return s.toString();
	}
	
	@Override
	public boolean equals(ApproxRelation o) {
		if (o instanceof ApproxLimitedRelation) {
			if (source.equals(((ApproxLimitedRelation) o).source)) {
				if (limit == ((ApproxLimitedRelation) o).limit) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public double samplingProbability() {
		VerdictLogger.warn(this, "sampling probability on LimitedRelation is meaningless.");
		return 0;
	}
	
}
