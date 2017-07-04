package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.FuncExpr;

public class NoApproxRelation extends ApproxRelation {
	
	ExactRelation r;

	public NoApproxRelation(ExactRelation r) {
		super(r.vc);
		this.r = r;
	}
	
	@Override
	public ExactRelation rewriteWithSubsampledErrorBounds() {
		return r;
	}

	@Override
	public ExactRelation rewriteForPointEstimate() {
		return r;
	}

	@Override
	protected ExactRelation rewriteWithPartition() {
		return r;
	}

	@Override
	protected ColNameExpr partitionColumn() {
		return null;
	}

	@Override
	protected double samplingProbabilityFor(FuncExpr f) {
		return 1.0;
	}

	@Override
	protected String sampleType() {
		return "nosample";
	}

	@Override
	protected List<TableUniqueName> accumulateStratifiedSamples() {
		return new ArrayList<TableUniqueName>();
	}

	@Override
	protected List<String> sampleColumns() {
		return new ArrayList<String>();
	}

	@Override
	protected Map<String, String> tableSubstitution() {
		return new HashMap<String, String>();
	}

}
