package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.relation.expr.BinaryOpExpr;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.ConstantExpr;
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
		ExactRelation r = rewriteWithPartition(true);
		
		// construct a new list of select elements. the last element is __vpart, which should be omitted.
		// newElems and newAggs hold almost the same info; just replicate them to follow the structure
		// of AggregatedRelation-ProjectedRelation.
		List<SelectElem> newElems = new ArrayList<SelectElem>();
		List<Expr> newAggs = new ArrayList<Expr>();
		List<SelectElem> elems = ((ProjectedRelation) r).getSelectElems();
		for (int i = 0; i < elems.size() - 1; i++) {
			SelectElem elem = elems.get(i);
			if (!elem.isagg()) {
				newElems.add(new SelectElem(ColNameExpr.from(elem.getAlias()), elem.getAlias()));
			} else {
				if (elem.getAlias().equals(partitionSizeAlias)) {
					continue;
				}
				
				ColNameExpr est = new ColNameExpr(elem.getAlias(), r.getAliasName());
				ColNameExpr psize = new ColNameExpr(partitionSizeAlias, r.getAliasName());
				
				// average estimate
				Expr averaged = null;
				if (elem.getExpr().isCountDistinct()) {
					// for count-distinct (i.e., universe samples), weighted average should not be used.
					averaged = FuncExpr.round(FuncExpr.avg(est));
				} else {
					// weighted average
					averaged = BinaryOpExpr.from(FuncExpr.sum(BinaryOpExpr.from(est, psize, "*")),
		                    					  FuncExpr.sum(psize), "/");
					if (elem.getExpr().isCount()) {
						averaged = FuncExpr.round(averaged);
					}
				}
				newElems.add(new SelectElem(averaged, elem.getAlias()));
				newAggs.add(averaged);
				
				// error estimation
				// scale by sqrt(subsample size) / sqrt(sample size)
				Expr error = BinaryOpExpr.from(
								BinaryOpExpr.from(FuncExpr.stddev(est), FuncExpr.sqrt(FuncExpr.avg(psize)), "*"),
								FuncExpr.sqrt(FuncExpr.sum(psize)),
								"/");
				error = BinaryOpExpr.from(error, ConstantExpr.from(confidenceIntervalMultiplier()), "*");
				newElems.add(new SelectElem(error, Relation.errorBoundColumn(elem.getAlias())));
				newAggs.add(error);
			}
		}
		
		// this extra aggregation stage should be grouped by non-agg elements except for __vpart
		List<Expr> newGroupby = new ArrayList<Expr>();
		for (SelectElem elem : elems) {
			if (!elem.isagg() && !elem.getAlias().equals(partitionColumnName())) {
				newGroupby.add(ColNameExpr.from(elem.getAlias()));
			}
		}
		r = new GroupedRelation(vc, r, newGroupby);
		r = new AggregatedRelation(vc, r, newAggs);
		r = new ProjectedRelation(vc, r, newElems);
		
		return r;
	}
	
//	@Override
//	public ExactRelation rewriteWithSubsampledErrorBounds() {
//		ExactRelation newSource = source.rewriteWithSubsampledErrorBounds();
//		List<SelectElem> sourceElems = null; // newSource.getSelectList();
//		Set<String> colAliases = new HashSet<String>();
//		for (SelectElem e : sourceElems) {
//			if (e.aliasPresent()) {
//				// we're only interested in the columns for which aliases are present.
//				// note that every column with aggregate function must have an alias (enforced by ColNameExpr class).
//				colAliases.add(e.getAlias());
//			}
//		}
//		
//		// we search for error bound columns based on the assumption that the error bound columns have the suffix attached
//		// to the original agg columns. The suffix is obtained from the ApproxRelation#errColSuffix() method.
//		// ApproxAggregatedRelation#rewriteWithSubsampledErrorBounds() method is responsible for having those columns. 
//		List<SelectElem> elemsWithErr = new ArrayList<SelectElem>();
//		for (SelectElem e : elems) {
//			elemsWithErr.add(e);
//			String errColName = errColName(e.getExpr().getText());
//			if (colAliases.contains(errColName)) {
//				elemsWithErr.add(new SelectElem(new ColNameExpr(errColName), errColName));
//			}
//		}
//		
//		ExactRelation r = new ProjectedRelation(vc, newSource, elemsWithErr);
//		r.setAliasName(getAliasName());
//		return r;
//	}
	
	/**
	 * Returns an ExactProjectRelation instance. The returned relation must include the partition column.
	 * If the source relation is an ApproxAggregatedRelation, we can expect that an extra groupby column is inserted for
	 * propagating the partition column.
	 */
	@Override
	protected ExactRelation rewriteWithPartition() {
		return rewriteWithPartition(false);
	}
	
	/**
	 * Inserts extra information if extra is set to true. The extra information is:
	 * 1. partition size. 
	 * @param extra
	 * @return
	 */
	protected ExactRelation rewriteWithPartition(boolean extra) {
		ExactRelation newSource = source.rewriteWithPartition();
		List<SelectElem> newElems = new ArrayList<SelectElem>();
		
		int index = 0;
		for (SelectElem elem : elems) {
			// we insert the non-agg element as it is
			// for an agg element, we found the expression in the source relation.
			// if there exists an agg element, source relation must be an instance of AggregatedRelation.
			if (!elem.getExpr().isagg()) {
				newElems.add(elem);
			} else {
				Expr agg = ((AggregatedRelation) newSource).getAggList().get(index++);
				newElems.add(new SelectElem(agg, elem.getAlias()));
//				Expr agg_err = ((AggregatedRelation) newSource).getAggList().get(index++);
//				newElems.add(new SelectElem(agg_err, Relation.errorBoundColumn(elem.getAlias())));
			}
		}
		
		// partition size only if the newSource is an instance of AggregatedRelation
		if (extra) {
			newElems.add(new SelectElem(FuncExpr.count(), partitionSizeAlias));
		}
		
		// partition number
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
