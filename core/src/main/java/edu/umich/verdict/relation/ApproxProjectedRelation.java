package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.SelectElem;

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
		List<SelectElem> sourceElems = null; // newSource.getSelectList();
		Set<String> colAliases = new HashSet<String>();
		for (SelectElem e : sourceElems) {
			if (e.aliasPresent()) {
				// we're only interested in the columns for which aliases are present.
				// note that every column with aggregate function must have an alias (enforced by ColNameExpr class).
				colAliases.add(e.getAlias());
			}
		}
		
		// we search for error bound columns based on the assumption that the error bound columns have the suffix attached
		// to the original agg columns. The suffix is obtained from the ApproxRelation#errColSuffix() method.
		// ApproxAggregatedRelation#rewriteWithSubsampledErrorBounds() method is responsible for having those columns. 
		List<SelectElem> elemsWithErr = new ArrayList<SelectElem>();
		for (SelectElem e : elems) {
			elemsWithErr.add(e);
			String errColName = errColName(e.getExpr().getText());
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
		ExactRelation newSource = source.rewriteWithPartition();
		List<SelectElem> newElems = new ArrayList<SelectElem>(elems);
		newElems.add(new SelectElem(newSource.partitionColumn(), partitionColumnName()));
		ExactRelation r = new ProjectedRelation(vc, newSource, newElems);
		r.setAliasName(getAliasName());
		return r;
	}
	
	@Override
	protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
		List<Expr> exprs = source.samplingProbabilityExprsFor(f);
		List<Expr> exprsWithNewAlias = new ArrayList<Expr>();
		for (Expr e : exprs) {
			if (e instanceof ColNameExpr) {
				exprsWithNewAlias.add(new ColNameExpr(((ColNameExpr) e).getCol(), alias));
			} else {
				exprsWithNewAlias.add(e);
			}
		}
		return exprsWithNewAlias;
	}

	@Override
	protected Map<String, String> tableSubstitution() {
		return source.tableSubstitution();
	}

	@Override
	public String sampleType() {
		return source.sampleType();
	}
	
	@Override
	public double cost() {
		return source.cost();
	}

	@Override
	protected List<String> sampleColumns() {
		return source.sampleColumns();
	}
	
	@Override
	protected String toStringWithIndent(String indent) {
		StringBuilder s = new StringBuilder(1000);
		s.append(indent);
		s.append(String.format("%s(%s) [%s]\n", this.getClass().getSimpleName(), getAliasName(), Joiner.on(", ").join(elems)));
		s.append(source.toStringWithIndent(indent + "  "));
		return s.toString();
	}
	
	@Override
	public boolean equals(ApproxRelation o) {
		if (o instanceof ApproxProjectedRelation) {
			if (source.equals(((ApproxProjectedRelation) o).source)) {
				if (elems.equals(((ApproxProjectedRelation) o).elems)) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public double samplingProbability() {
		return source.samplingProbability();
	}

}
