package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
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
    
//    public static ApproxProjectedRelation from(VerdictContext vc, ApproxAggregatedRelation r) {
//        List<SelectElem> selectElems = new ArrayList<SelectElem>();
//
//        // groupby expressions
//        if (r.getSource() != null) {
//            Pair<List<Expr>, ApproxRelation> groupbyAndPreceding = allPrecedingGroupbys(r.getSource());
//            List<Expr> groupby = groupbyAndPreceding.getLeft();
//            for (Expr e : groupby) {
//                selectElems.add(new SelectElem(vc, e));
//            }
//        }
//
//        // aggregate expressions
//        for (Expr e : r.getAggList()) {
//            selectElems.add(new SelectElem(vc, e));     // automatically aliased
//        }
//        ApproxProjectedRelation rel = new ApproxProjectedRelation(vc, r, selectElems);
//        return rel;
//    }

    @Override
    public ExactRelation rewriteForPointEstimate() {
        ExactRelation r = new ProjectedRelation(vc, source.rewriteForPointEstimate(), elems);
        r.setAlias(getAlias());
        return r;
    }

//    private List<SelectElem> elemsWithSubstitutedTables() {
//        List<SelectElem> newElems = new ArrayList<SelectElem>();
//        Map<TableUniqueName, String> sub = source.tableSubstitution();
//        for (SelectElem elem : elems) {
//            Expr newExpr = exprWithTableNamesSubstituted(elem.getExpr(), sub);
//            SelectElem newElem = new SelectElem(vc, newExpr, elem.getAlias());
//            newElems.add(newElem);
//        }
//        return newElems;
//    }

    @Override
    public ExactRelation rewriteWithSubsampledErrorBounds() {
        // if this is not an approximate relation effectively, we don't need any special rewriting.
        if (!doesIncludeSample()) {
            return getOriginalRelation();
        }
        
        ExactRelation r = new ProjectedRelation(vc, source.rewriteWithSubsampledErrorBounds(), elems);
        r.setAlias(getAlias());
        return r;
        
//        // if the source is not an aggregated relation, we do not need any special treatment.
//        if (!(source instanceof ApproxAggregatedRelation)) {
//            ExactRelation r = new ProjectedRelation(vc, source.rewriteWithSubsampledErrorBounds(), elemsWithSubstitutedTables());
//            r.setAlias(getAlias());
//            return r;
//        }
//
//        ExactRelation r = rewriteWithPartition(true);
//
//        // construct a new list of select elements. the last element is __vpart, which should be omitted.
//        // newElems and newAggs hold almost the same info; just replicate them to follow the structure
//        // of AggregatedRelation-ProjectedRelation.
//        List<SelectElem> newElems = new ArrayList<SelectElem>();
//        List<Expr> newAggs = new ArrayList<Expr>();
//        List<SelectElem> elems = ((ProjectedRelation) r).getSelectElems();
//
//        for (int i = 0; i < elems.size() - 1; i++) {
//            SelectElem elem = elems.get(i);
//            Optional<SelectElem> originalElem = Optional.absent();
//            if (i < this.elems.size()) {
//                originalElem = Optional.fromNullable(this.elems.get(i));
//            }
//
//            if (!elem.isagg()) {
//                // skip the partition number
//                if (elem.aliasPresent() && elem.getAlias().equals(partitionColumnName())) {
//                    continue;
//                }
//
//                SelectElem newElem = null;
//                if (elem.getAlias() == null) {
//                    Expr newExpr = elem.getExpr().withTableSubstituted(r.getAlias());
//                    newElem = new SelectElem(vc, newExpr, elem.getAlias());
//                } else {
//                    newElem = new SelectElem(vc, new ColNameExpr(vc, elem.getAlias(), r.getAlias()), elem.getAlias());
//                }
//                newElems.add(newElem);
//            } else {
//                if (elem.getAlias().equals(partitionSizeAlias)) {
//                    continue;
//                }
//
//                ColNameExpr est = new ColNameExpr(vc, elem.getAlias(), r.getAlias());
//                ColNameExpr psize = new ColNameExpr(vc, partitionSizeAlias, r.getAlias());
//
//                // average estimate
//                Expr averaged = null;
//                if (originalElem.isPresent() && originalElem.get().getExpr().isCountDistinct()) {
//                    // for count-distinct (i.e., universe samples), weighted average should not be used.
//                    averaged = FuncExpr.round(FuncExpr.avg(est));
//                } else {
//                    // weighted average
//                    averaged = BinaryOpExpr.from(vc, FuncExpr.sum(BinaryOpExpr.from(vc, est, psize, "*")),
//                            FuncExpr.sum(psize), "/");
//                    if (originalElem.isPresent() && originalElem.get().getExpr().isCount()) {
//                        averaged = FuncExpr.round(averaged);
//                    }
//                }
//                newElems.add(new SelectElem(vc, averaged, elem.getAlias()));
//                newAggs.add(averaged);
//
//                // error estimation
//                // scale by sqrt(subsample size) / sqrt(sample size)
//                Expr error = BinaryOpExpr.from(vc,
//                        BinaryOpExpr.from(vc, FuncExpr.stddev(est), FuncExpr.sqrt(FuncExpr.avg(psize)), "*"),
//                        FuncExpr.sqrt(FuncExpr.sum(psize)),
//                        "/");
//                error = BinaryOpExpr.from(vc, error, ConstantExpr.from(vc, confidenceIntervalMultiplier()), "*");
//                newElems.add(new SelectElem(vc, error, Relation.errorBoundColumn(elem.getAlias())));
//                newAggs.add(error);
//            }
//        }
//
//        // this extra aggregation stage should be grouped by non-agg elements except for __vpart
//        List<Expr> newGroupby = new ArrayList<Expr>();
//        for (SelectElem elem : elems) {
//            if (!elem.isagg()) {
//                if (elem.aliasPresent()) {
//                    if (!elem.getAlias().equals(partitionColumnName())) {
//                        newGroupby.add(new ColNameExpr(vc, elem.getAlias(), r.getAlias()));
//                    }
//                } else {
//                    if (!elem.getExpr().toString().equals(partitionColumnName())) {
//                        newGroupby.add(elem.getExpr().withTableSubstituted(r.getAlias()));
//                    }
//                }
//            }
//        }
//        if (newGroupby.size() > 0) {
//            r = new GroupedRelation(vc, r, newGroupby);
//        }
//        r = new AggregatedRelation(vc, r, newAggs);
//        r = new ProjectedRelation(vc, r, newElems);
//
//        return r;
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
        ExactRelation newSource = source.rewriteWithPartition();
        List<SelectElem> newElems = new ArrayList<SelectElem>();
        newElems.addAll(elems);
        
        // prob column (for point estimate and also for subsampling)
        newElems.add(new SelectElem(vc, newSource.tupleProbabilityColumn(), samplingProbabilityColumnName()));
        
        // partition column (for subsampling)
        newElems.add(new SelectElem(vc, newSource.partitionColumn(), partitionColumnName()));
        
        ExactRelation r = new ProjectedRelation(vc, newSource, newElems);
        r.setAlias(getAlias());
        return r;
        
//        return rewriteWithPartition(false);
    }

//    /**
//     * Inserts extra information if extra is set to true. The extra information is:
//     * 1. partition size.
//     * @param extra
//     * @return
//     */
//    protected ExactRelation rewriteWithPartition() {
//        ExactRelation newSource = source.rewriteWithPartition();
//        List<SelectElem> newElems = new ArrayList<SelectElem>();
//        Map<TableUniqueName, String> sub = source.tableSubstitution();
//
//        int index = 0;
//        for (SelectElem elem : elems) {
//            // we insert the non-agg element as it is
//            // for an agg element, we found the expression in the source relation.
//            // if there exists an agg element, source relation must be an instance of AggregatedRelation.
//            if (!elem.getExpr().isagg()) {
//                Expr newExpr = exprWithTableNamesSubstituted(elem.getExpr(), sub);	// replace original table references with samples
//                SelectElem newElem = new SelectElem(vc, newExpr, elem.getAlias());
//                newElems.add(newElem);
//            } else {
//                Expr agg = ((AggregatedRelation) newSource).getAggList().get(index++);
//                agg = exprWithTableNamesSubstituted(agg, sub);			// replace original table references with samples
//                newElems.add(new SelectElem(vc, agg, elem.getAlias()));
//                //				Expr agg_err = ((AggregatedRelation) newSource).getAggList().get(index++);
//                //				newElems.add(new SelectElem(agg_err, Relation.errorBoundColumn(elem.getAlias())));
//            }
//        }
//
//        // partition size column; used for combining the final answer computed on different partitions.
//        if (extra) {
//            newElems.add(new SelectElem(vc, FuncExpr.count(), partitionSizeAlias));
//        }
//
//        // partition number
//        newElems.add(new SelectElem(vc, newSource.partitionColumn(), partitionColumnName()));
//
//        // probability expression
//        //  if the source is not an aggregated relation, we simply propagates the probability expression.
//        //  if the source is an aggregated relation, we should insert an appropriate value.
//        String probCol = samplingProbabilityColumnName();
//        if (!(source instanceof ApproxAggregatedRelation)) {
//            SelectElem probElem = new SelectElem(vc, new ColNameExpr(vc, probCol), probCol);
//            newElems.add(probElem);
//        } else {
//            ApproxRelation a = ((ApproxAggregatedRelation) source).getSource();
//            if (!(a instanceof ApproxGroupedRelation)) {
//                SelectElem probElem = new SelectElem(vc, new ConstantExpr(vc, 1.0), probCol);
//                newElems.add(probElem);
//            } else {
//                if (source.sampleType().equals("universe")) {
//                    SelectElem probElem = new SelectElem(vc, new ConstantExpr(vc, source.samplingProbability()), probCol);
//                    newElems.add(probElem);
//                } else if (source.sampleType().equals("stratified")) {
//                    SelectElem probElem = new SelectElem(vc, new ConstantExpr(vc, 1.0), probCol);
//                    newElems.add(probElem);
//                } else {
//                    SelectElem probElem = new SelectElem(vc, new ConstantExpr(vc, 1.0), probCol);
//                    newElems.add(probElem);
//                }
//            }
//        }
//
//        ExactRelation r = new ProjectedRelation(vc, newSource, newElems);
//        r.setAlias(getAlias());
//        return r;
//    }

    @Override
    protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
        List<Expr> exprs = source.samplingProbabilityExprsFor(f);
        List<Expr> exprsWithNewAlias = new ArrayList<Expr>();
        for (Expr e : exprs) {
            if (e instanceof ColNameExpr) {
                exprsWithNewAlias.add(new ColNameExpr(vc, ((ColNameExpr) e).getCol(), alias));
            } else {
                exprsWithNewAlias.add(e);
            }
        }
        return exprsWithNewAlias;
    }

    /**
     * Due to the fact that the antecedents of a projected relation does not propagate any substitution.
     */
    @Override
    protected Map<TableUniqueName, String> tableSubstitution() {
        return ImmutableMap.of();
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
        s.append(String.format("%s(%s) [%s] type: %s\n", 
                 this.getClass().getSimpleName(), 
                 getAlias(), 
                 Joiner.on(", ").join(elems),
                 sampleType()));
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

    @Override
    protected boolean doesIncludeSample() {
        return source.doesIncludeSample();
    }

}
