package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.datatypes.SampleSizeInfo;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.relation.expr.Expr;
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
	protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
		return Arrays.asList();
	}

	@Override
	public String sampleType() {
		return "nosample";
	}
	
	@Override
	public double cost() {
		return 1e6;		// TODO: a better alternative?
	}

	@Override
	protected List<String> sampleColumns() {
		return new ArrayList<String>();
	}

	@Override
	protected Map<TableUniqueName, String> tableSubstitution() {
		return ImmutableMap.of();
	}

	@Override
	protected String toStringWithIndent(String indent) {
		StringBuilder s = new StringBuilder(1000);
		s.append(indent);
		s.append(String.format("%s(%s)\n", this.getClass().getSimpleName(), getAlias()));
		s.append(r.toStringWithIndent(indent + "  "));
		return s.toString();
	}
	
	@Override
	public boolean equals(ApproxRelation o) {
		if (o instanceof NoApproxRelation) {
			return r.equals(((NoApproxRelation) o).r);
		} else {
			return false;
		}
	}

	@Override
	public double samplingProbability() {
		return 1.0;
	}
}
