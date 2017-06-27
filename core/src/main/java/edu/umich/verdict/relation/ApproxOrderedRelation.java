package edu.umich.verdict.relation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.relation.expr.ColNameExpr;
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
	}

	@Override
	public ExactRelation rewrite() {
		ExactRelation r = new OrderedRelation(vc, source.rewrite(), orderby);
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
