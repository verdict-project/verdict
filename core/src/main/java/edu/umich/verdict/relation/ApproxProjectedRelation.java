/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    // public static ApproxProjectedRelation from(VerdictContext vc,
    // ApproxAggregatedRelation r) {
    // List<SelectElem> selectElems = new ArrayList<SelectElem>();
    //
    // // groupby expressions
    // if (r.getSource() != null) {
    // Pair<List<Expr>, ApproxRelation> groupbyAndPreceding =
    // allPrecedingGroupbys(r.getSource());
    // List<Expr> groupby = groupbyAndPreceding.getLeft();
    // for (Expr e : groupby) {
    // selectElems.add(new SelectElem(vc, e));
    // }
    // }
    //
    // // aggregate expressions
    // for (Expr e : r.getAggList()) {
    // selectElems.add(new SelectElem(vc, e)); // automatically aliased
    // }
    // ApproxProjectedRelation rel = new ApproxProjectedRelation(vc, r,
    // selectElems);
    // return rel;
    // }

    @Override
    public ExactRelation rewriteForPointEstimate() {
        ExactRelation r = new ProjectedRelation(vc, source.rewriteForPointEstimate(), elems);
        r.setAlias(getAlias());
        return r;
    }

    // private List<SelectElem> elemsWithSubstitutedTables() {
    // List<SelectElem> newElems = new ArrayList<SelectElem>();
    // Map<TableUniqueName, String> sub = source.tableSubstitution();
    // for (SelectElem elem : elems) {
    // Expr newExpr = exprWithTableNamesSubstituted(elem.getExpr(), sub);
    // SelectElem newElem = new SelectElem(vc, newExpr, elem.getAlias());
    // newElems.add(newElem);
    // }
    // return newElems;
    // }

    @Override
    public ExactRelation rewriteWithSubsampledErrorBounds() {
        // if this is not an approximate relation effectively, we don't need any special
        // rewriting.
        if (!doesIncludeSample()) {
            return getOriginalRelation();
        }

        ExactRelation r = new ProjectedRelation(vc, source.rewriteWithSubsampledErrorBounds(), elems);
        r.setAlias(getAlias());
        return r;
    }

    /**
     * Returns an ExactProjectRelation instance. The returned relation must include
     * the partition column. If the source relation is an ApproxAggregatedRelation,
     * we can expect that an extra groupby column is inserted for propagating the
     * partition column.
     */
    @Override
    protected ExactRelation rewriteWithPartition() {
        ExactRelation newSource = source.rewriteWithPartition();
        List<SelectElem> newElems = new ArrayList<SelectElem>();
        newElems.addAll(elems);

        // prob column (for point estimate and also for subsampling)
        newElems.add(new SelectElem(vc, source.tupleProbabilityColumn(), samplingProbabilityColumnName()));

        // partition column (for subsampling)
        newElems.add(new SelectElem(vc, newSource.partitionColumn(), partitionColumnName()));

        ExactRelation r = new ProjectedRelation(vc, newSource, newElems);
        r.setAlias(getAlias());
        return r;

        // return rewriteWithPartition(false);
    }

    // /**
    // * Inserts extra information if extra is set to true. The extra information
    // is:
    // * 1. partition size.
    // * @param extra
    // * @return
    // */
    // protected ExactRelation rewriteWithPartition() {
    // ExactRelation newSource = source.rewriteWithPartition();
    // List<SelectElem> newElems = new ArrayList<SelectElem>();
    // Map<TableUniqueName, String> sub = source.tableSubstitution();
    //
    // int index = 0;
    // for (SelectElem elem : elems) {
    // // we insert the non-agg element as it is
    // // for an agg element, we found the expression in the source relation.
    // // if there exists an agg element, source relation must be an instance of
    // AggregatedRelation.
    // if (!elem.getExpr().isagg()) {
    // Expr newExpr = exprWithTableNamesSubstituted(elem.getExpr(), sub); // replace
    // original table references with samples
    // SelectElem newElem = new SelectElem(vc, newExpr, elem.getAlias());
    // newElems.add(newElem);
    // } else {
    // Expr agg = ((AggregatedRelation) newSource).getAggList().get(index++);
    // agg = exprWithTableNamesSubstituted(agg, sub); // replace original table
    // references with samples
    // newElems.add(new SelectElem(vc, agg, elem.getAlias()));
    // // Expr agg_err = ((AggregatedRelation) newSource).getAggList().get(index++);
    // // newElems.add(new SelectElem(agg_err,
    // Relation.errorBoundColumn(elem.getAlias())));
    // }
    // }
    //
    // // partition size column; used for combining the final answer computed on
    // different partitions.
    // if (extra) {
    // newElems.add(new SelectElem(vc, FuncExpr.count(), partitionSizeAlias));
    // }
    //
    // // partition number
    // newElems.add(new SelectElem(vc, newSource.partitionColumn(),
    // partitionColumnName()));
    //
    // // probability expression
    // // if the source is not an aggregated relation, we simply propagates the
    // probability expression.
    // // if the source is an aggregated relation, we should insert an appropriate
    // value.
    // String probCol = samplingProbabilityColumnName();
    // if (!(source instanceof ApproxAggregatedRelation)) {
    // SelectElem probElem = new SelectElem(vc, new ColNameExpr(vc, probCol),
    // probCol);
    // newElems.add(probElem);
    // } else {
    // ApproxRelation a = ((ApproxAggregatedRelation) source).getSource();
    // if (!(a instanceof ApproxGroupedRelation)) {
    // SelectElem probElem = new SelectElem(vc, new ConstantExpr(vc, 1.0), probCol);
    // newElems.add(probElem);
    // } else {
    // if (source.sampleType().equals("universe")) {
    // SelectElem probElem = new SelectElem(vc, new ConstantExpr(vc,
    // source.samplingProbability()), probCol);
    // newElems.add(probElem);
    // } else if (source.sampleType().equals("stratified")) {
    // SelectElem probElem = new SelectElem(vc, new ConstantExpr(vc, 1.0), probCol);
    // newElems.add(probElem);
    // } else {
    // SelectElem probElem = new SelectElem(vc, new ConstantExpr(vc, 1.0), probCol);
    // newElems.add(probElem);
    // }
    // }
    // }
    //
    // ExactRelation r = new ProjectedRelation(vc, newSource, newElems);
    // r.setAlias(getAlias());
    // return r;
    // }

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
     * Due to the fact that the antecedents of a projected relation does not
     * propagate any substitution.
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
        s.append(String.format("%s(%s) [%s] type: %s\n", this.getClass().getSimpleName(), getAlias(),
                Joiner.on(", ").join(elems), sampleType()));
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

    @Override
    public Expr tupleProbabilityColumn() {
        return new ColNameExpr(vc, samplingProbabilityColumnName(), getAlias());
    }

    @Override
    public Expr tableSamplingRatio() {
        return new ColNameExpr(vc, samplingRatioColumnName(), getAlias());
    }

}
