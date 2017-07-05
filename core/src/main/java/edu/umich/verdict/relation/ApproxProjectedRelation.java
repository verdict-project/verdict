package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.ExprVisitor;
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
		ExactRelation newSource = source.rewriteWithSubsampledErrorBounds();
		List<SelectElem> sourceElems = newSource.getSelectList();
		Set<String> colAliases = new HashSet<String>();
		for (SelectElem e : sourceElems) {
			if (e.aliasPresent()) {
				// we're only interested in the columns for which aliases are present.
				// note that every column with aggregate function must have an alias (enforced by ColNameExpr class).
				colAliases.add(e.getAlias());
			}
		}
		
		// we look for error bound columns based on the assumption that the error bound columns have the suffix attached
		// to the original agg columns. The suffix is obtained from the ApproxRelation#errColSuffix() method.
		// ApproxAggregatedRelation#rewriteWithSubsampledErrorBounds() method is responsible for having those columns. 
		List<SelectElem> elemsWithErr = new ArrayList<SelectElem>();
		for (SelectElem e : elems) {
			elemsWithErr.add(e);
			String errColName = e.getExpr().toString() + errColSuffix();
			if (colAliases.contains(errColName)) {
				elemsWithErr.add(new SelectElem(new ColNameExpr(errColName), errColName));
			}
		}
		
		ExactRelation r = new ProjectedRelation(vc, newSource, elemsWithErr);
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
	protected List<String> sampleColumns() {
		return source.sampleColumns();
	}
	
}
