package edu.umich.verdict.relation;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.SelectElem;
import edu.umich.verdict.util.VerdictLogger;

public class ApproxProjectedRelation extends ApproxRelation {
	
	private ApproxRelation source; 

	private List<SelectElem> elems;

	public ApproxProjectedRelation(VerdictContext vc, ApproxRelation source, List<SelectElem> elems) {
		super(vc);
		this.source = source;
		this.elems = elems;
	}
	
	public List<SelectElem> getSelectElems() {
		return elems;
	}

	@Override
	public ExactRelation rewriteForPointEstimate() {
		ExactRelation r = new ProjectedRelation(vc, source.rewriteForPointEstimate(), elems);
		r.setAliasName(getAliasName());
		return r;
	}
	
	@Override
	public ExactRelation rewriteWithSubsampledErrorBounds() {
		ExactRelation r = new ProjectedRelation(vc, source.rewriteWithSubsampledErrorBounds(), elems);
		r.setAliasName(getAliasName());
		return r;
	}
	
	@Override
	public ExactRelation rewriteWithPartition() {
		ExactRelation r = new ProjectedRelation(vc, source.rewriteWithPartition(), elems);
		r.setAliasName(getAliasName());
		return r;
	}
	
	@Override
	protected ColNameExpr partitionColumn() {
		return source.partitionColumn();
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
		return source.sampleType();
	}
	
	@Override
	protected List<TableUniqueName> accumulateStratifiedSamples() {
		return source.accumulateStratifiedSamples();
	}

	@Override
	protected List<String> sampleColumns() {
		return source.sampleColumns();
	}

}
