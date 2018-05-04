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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.condition.IsCond;
import edu.umich.verdict.relation.condition.NullCond;
import edu.umich.verdict.relation.expr.*;
import edu.umich.verdict.util.VerdictLogger;
import org.apache.commons.lang3.tuple.Pair;

public class ApproxAggregatedRelation extends ApproxRelation {

    private ApproxRelation source;

    private List<SelectElem> elems;

    private boolean includeGroupsInToSql = true;

    public ApproxAggregatedRelation(VerdictContext vc, ApproxRelation source, List<SelectElem> elems) {
        super(vc);
        this.source = source;
        this.elems = elems;
    }

    public ApproxRelation getSource() {
        return source;
    }

    public List<SelectElem> getElemList() {
        return elems;
    }

    public List<SelectElem> getAggElemList() {
        List<SelectElem> aggs = new ArrayList<SelectElem>();
        for (SelectElem elem : elems) {
            if (elem.isagg()) {
                aggs.add(elem);
            }
        }
        return aggs;
    }

    public void setIncludeGroupsInToSql(boolean o) {
        includeGroupsInToSql = o;
    }

    @Override
    public ExactRelation rewriteForPointEstimate() {
        ExactRelation newSource = source.rewriteForPointEstimate();

        List<SelectElem> scaled = new ArrayList<SelectElem>();
        // List<ColNameExpr> samplingProbColumns =
        // newSource.accumulateSamplingProbColumns();
        for (SelectElem elem : elems) {
            if (!elem.isagg()) {
                scaled.add(elem);
            } else {
                Expr agg = elem.getExpr();
                scaled.add(new SelectElem(vc, transformForSingleFunction(agg), elem.getAlias()));
            }
        }
        ExactRelation r = new AggregatedRelation(vc, newSource, scaled);
        r.setAlias(getAlias());
        return r;
    }

    @Override
    public ExactRelation rewriteWithSubsampledErrorBounds() {
        // if this is not an approximate relation effectively, we don't need any special
        // rewriting.
        if (!doesIncludeSample()) {
            return getOriginalRelation();
        }

        ExactRelation r = rewriteWithPartition(true);
        // String newAlias = genTableAlias();

        // put another layer to combine per-partition aggregates
        List<SelectElem> newElems = new ArrayList<SelectElem>();
        List<SelectElem> oldElems = ((AggregatedRelation) r).getElemList();
        List<Expr> newGroupby = new ArrayList<Expr>();

        for (int i = 0; i < oldElems.size(); i++) {
            SelectElem elem = oldElems.get(i);

            // used to identify the original aggregation type
            Optional<SelectElem> originalElem = Optional.absent();
            if (i < this.elems.size()) {
                originalElem = Optional.fromNullable(this.elems.get(i));
            }

            if (!elem.isagg()) {
                // skip the partition number
                if (elem.aliasPresent() && elem.getAlias().equals(partitionColumnName())) {
                    continue;
                }

                SelectElem newElem = null;
                Expr newExpr = null;
                if (elem.getAlias() == null) {
                    newExpr = elem.getExpr().withTableSubstituted(r.getAlias());
                    newElem = new SelectElem(vc, newExpr, elem.getAlias());
                } else {
                    newExpr = new ColNameExpr(vc, elem.getAlias(), r.getAlias());
                    newElem = new SelectElem(vc, newExpr, elem.getAlias());
                }

                // groupby element may not be present in the select list.
                if (originalElem.isPresent()) {
                    newElems.add(newElem);
                }
                newGroupby.add(newExpr);
            } else {
                // skip the partition size column
                if (elem.getAlias().equals(partitionSizeAlias)) {
                    continue;
                }

                // skip the extra columns inserted
                if (!originalElem.isPresent()) {
                    continue;
                }

                ColNameExpr est = new ColNameExpr(vc, elem.getAlias(), r.getAlias());
                ColNameExpr psize = new ColNameExpr(vc, partitionSizeAlias, r.getAlias());

                // average estimate
                Expr averaged = null;
                Expr originalExpr = originalElem.get().getExpr();
                if (originalExpr.isCountDistinct()) {
                    // for count-distinct (i.e., universe samples), weighted average should not be
                    // used.
                    averaged = FuncExpr.round(FuncExpr.avg(est));
                } else if (originalExpr.isMax()) {
                    averaged = FuncExpr.max(est);
                } else if (originalExpr.isMin()) {
                    averaged = FuncExpr.min(est);
                } else {
                    // weighted average
                    averaged = BinaryOpExpr.from(vc, FuncExpr.sum(BinaryOpExpr.from(vc, est, psize, "*")),
                            FuncExpr.sum(psize), "/");
                    if (originalElem.get().getExpr().isCount()) {
                        averaged = FuncExpr.round(averaged);
                    }
                }
                newElems.add(new SelectElem(vc, averaged, elem.getAlias()));

                // error estimation
                // scale by sqrt(subsample size) / sqrt(sample size)
                if (originalExpr.isMax() || originalExpr.isMin()) {
                    // no error estimations for extreme statistics
                } else {
                    Expr error = BinaryOpExpr.from(vc,
                            BinaryOpExpr.from(vc, FuncExpr.stddev(est), FuncExpr.sqrt(FuncExpr.avg(psize)), "*"),
                            FuncExpr.sqrt(FuncExpr.sum(psize)), "/");
                    error = BinaryOpExpr.from(vc, error, ConstantExpr.from(vc, confidenceIntervalMultiplier()), "*");
                    newElems.add(new SelectElem(vc, error, Relation.errorBoundColumn(elem.getAlias())));
                }
            }
        }

        // this extra aggregation stage should be grouped by non-agg elements except for
        // __vpart
        // List<Expr> newGroupby = new ArrayList<Expr>();
        // if (source instanceof ApproxGroupedRelation) {
        // for (Expr g : ((ApproxGroupedRelation) source).getGroupby()) {
        // if (g instanceof ColNameExpr && ((ColNameExpr)
        // g).getCol().equals(partitionColumnName())) {
        // continue;
        // } else {
        // newGroupby.add(g.withTableSubstituted(r.getAlias()));
        // }
        // }
        // }

        // for (SelectElem elem : elems) {
        // if (!elem.isagg()) {
        // if (elem.aliasPresent()) {
        // if (!elem.getAlias().equals(partitionColumnName())) {
        // newGroupby.add(new ColNameExpr(vc, elem.getAlias(), r.getAlias()));
        // }
        // } else { // does not happen
        // if (!elem.getExpr().toString().equals(partitionColumnName())) {
        // newGroupby.add(elem.getExpr().withTableSubstituted(r.getAlias()));
        // }
        // }
        // }
        // }
        if (newGroupby.size() > 0) {
            r = new GroupedRelation(vc, r, newGroupby);
        }

        r = new AggregatedRelation(vc, r, newElems);
        r.setAlias(getAlias());
        return r;
    }

    /**
     * This relation must include partition numbers, and the answers must be scaled
     * properly. Note that {@link ApproxRelation#rewriteWithSubsampledErrorBounds()}
     * is used only for the statement including final error bounds; all internal
     * manipulations must be performed by this method.
     * 
     * The rewritten relation transforms original aggregate elements as follows.
     * Every aggregate element is replaced with two aggregate elements. One is for
     * mean estimate and the other is for error estimate.
     * 
     * The rewritten relation includes an extra aggregate element: count(*). This is
     * to compute the partition sizes. These partition sizes can be used by an
     * upstream (or parent) relation for computing the final mean estimate. (note
     * that computing weighted average provides higher accuracy compared to
     * unweighted average.)
     * 
     * @return
     */
    @Override
    protected ExactRelation rewriteWithPartition() {
        return rewriteWithPartition(false);
    }

    /**
     * Partitioned-aggregation
     * 
     * select list elements are scaled considering both sampling probabilities and
     * partitioning for subsampling.
     * @param projectUnprojectedGroups
     *            This option is used by
     *            {@link ApproxAggregatedRelation#rewriteWithSubsampledErrorBounds()}.
     * @return
     */
    protected ExactRelation rewriteWithPartition(boolean projectUnprojectedGroups) {
        ExactRelation newSource = partitionedSource();

        List<SelectElem> scaledElems = new ArrayList<SelectElem>();
        List<Expr> groupby = new ArrayList<Expr>();
        if (source instanceof ApproxGroupedRelation) {
            groupby.addAll(((ApproxGroupedRelation) source).getGroupby());
        }
        ColNameExpr partitionColExpr = newSource.partitionColumn();
        Expr tupleSamplingProbExpr = source.tupleProbabilityColumn();
        Expr tableSamplingRatioExpr = source.tableSamplingRatio();
        //get the table name in the from clause
        Pair<List<Expr>, ApproxRelation> groupsAndNextR = allPrecedingGroupbys(this.source);
        List<Expr> group = groupsAndNextR.getLeft();
        Pair<Optional<Cond>, ApproxRelation> filtersAndNextR = allPrecedingFilters(groupsAndNextR.getRight());
        String csql =  (filtersAndNextR.getLeft().isPresent()) ? filtersAndNextR.getLeft().get().withNewTablePrefix("tmp_").toString() : "";
        SingleFunctionTransformerForSubsampling transformer = new SingleFunctionTransformerForSubsampling(vc, groupby,
                partitionColExpr, tupleSamplingProbExpr, tableSamplingRatioExpr, csql);

        // copies groupby expressions (used if projectUnprojectedGroups is set to true)
        // this extra groupby is needed when the user-submitted query does not include the groupby columns
        // explicitly (for some reason) in the query.
        // these extra grouping attributes will be projected out eventually.
        List<ColNameExpr> unappearingGroups = new ArrayList<ColNameExpr>();
        if (source instanceof ApproxGroupedRelation) {
            for (Expr e : ((ApproxGroupedRelation) source).getGroupby()) {
                if (e instanceof ColNameExpr) {
                    unappearingGroups.add((ColNameExpr) e);
                }
            }
        }

        for (SelectElem elem : elems) {
            if (!elem.isagg()) {
                // group-by attribute
                scaledElems.add(elem);

                // update unappearingGroups by removing the attributes that appeared
                Expr e = elem.getExpr();
                if (e instanceof ColNameExpr) {
                    int i = 0;
                    for (i = 0; i < unappearingGroups.size(); i++) {
                        if (unappearingGroups.get(i).getCol().equals(((ColNameExpr) e).getCol())) {
                            break;
                        }
                    }
                    if (i < unappearingGroups.size()) {
                        unappearingGroups.remove(i);
                    }
                }
            } else {
                Expr agg = elem.getExpr();
                // Expr scaled = transformForSingleFunctionWithPartitionSize(agg, groupby,
                // partitionColExpr, tupleSamplingProbExpr, tableSamplingRatio);
                Expr scaled = transformer.call(agg);
                scaledElems.add(new SelectElem(vc, scaled, elem.getAlias()));
            }
        }

        // project unappearing groups
        if (projectUnprojectedGroups) {
            for (ColNameExpr e : unappearingGroups) {
                scaledElems.add(new SelectElem(vc, e));
            }
        }

        // insert partition number if exists
        ColNameExpr partitionCol = newSource.partitionColumn();
        if (partitionCol != null) {
            scaledElems.add(new SelectElem(vc, newSource.partitionColumn(), partitionColumnName()));
        }

        // to compute the partition size
        scaledElems.add(new SelectElem(vc, FuncExpr.count(), partitionSizeAlias));

        // insert sampling probability column (tuple-level)
        scaledElems.add(new SelectElem(vc, FuncExpr.avg(ConstantExpr.from(vc, samplingProbability())),
                samplingProbabilityColumnName()));

        ExactRelation r = new AggregatedRelation(vc, newSource, scaledElems);
        r.setAlias(getAlias());

        return r;
    }

    private ExactRelation partitionedSource() {
        if (source instanceof ApproxGroupedRelation) {
            return source.rewriteWithPartition();
        } else {
            return (new ApproxGroupedRelation(vc, source, Arrays.<Expr>asList())).rewriteWithPartition();
        }
    }

    @Override
    protected Map<TableUniqueName, String> tableSubstitution() {
        return Collections.<TableUniqueName, String>emptyMap();
        // return source.tableSubstitution();
    }

    @Override
    protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
        if (f.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
            if (sampleType().equals("universe")) {
                return Arrays.<Expr>asList(ConstantExpr.from(vc, samplingProbability()));
            } else if (sampleType().equals("stratified")) {
                return Arrays.<Expr>asList();
            } else if (sampleType().equals("nosample")) {
                return Arrays.<Expr>asList();
            } else {
                VerdictLogger.warn(this,
                        String.format("%s sample should not be used for count-distinct.", sampleType()));
                return Arrays.<Expr>asList(ConstantExpr.from(vc, 1.0));
            }
        } else { // SUM, COUNT, AVG
            if (!sampleType().equals("nosample")) {
                String samplingProbCol = samplingProbabilityColumnName();
                return Arrays.<Expr>asList(new ColNameExpr(vc, samplingProbCol, alias));
            } else {
                return Arrays.<Expr>asList();
            }
        }
    }

    class SingleFunctionTransformerForSubsampling extends ExprModifier {

        final List<Expr> groupby;

        final ColNameExpr partitionColExpr;

        final Expr tupleSamplingProbExpr;

        final Expr tableSamplingRatioExpr;

        final String csql;

        public SingleFunctionTransformerForSubsampling(VerdictContext vc, List<Expr> groupby,
                ColNameExpr partitionColExpr, Expr tupleSamplingProbExpr, Expr tableSamplingRatioExpr, String csql) {
            super(vc);
            this.groupby = groupby;
            this.partitionColExpr = partitionColExpr;
            this.tupleSamplingProbExpr = tupleSamplingProbExpr;
            this.tableSamplingRatioExpr = tableSamplingRatioExpr;
            this.csql = csql;
        }

        public Expr call(Expr expr) {
            if (expr instanceof BinaryOpExpr) {
                BinaryOpExpr old = (BinaryOpExpr) expr;
                BinaryOpExpr newExpr = new BinaryOpExpr(expr.getVerdictContext(), visit(old.getLeft()),
                        visit(old.getRight()), old.getOp());
                return newExpr;
            } else if (expr instanceof FuncExpr) {
                // Take two different approaches:
                // 1. stratified samples: use the verdict_sampling_prob column (in each tuple).
                // 2. other samples: use either the uniform sampling probability or the ratio
                // between the sample
                // size and the original table size.

                FuncExpr f = (FuncExpr) expr;
                // FuncExpr s = (FuncExpr) exprWithTableNamesSubstituted(expr,
                // tupleSamplingProbExpr);
                // List<Expr> samplingProbExprs = source.samplingProbabilityExprsFor(f);

                if (f.getFuncName().equals(FuncExpr.FuncName.COUNT)) {
                    // scale with sampling probability
                    Expr scaled = FuncExpr
                            .sum(BinaryOpExpr.from(vc, ConstantExpr.from(vc, 1.0), tupleSamplingProbExpr, "/"));
                    // scale with partition size
                    scaled = BinaryOpExpr.from(vc, scaled, FuncExpr.count(), "/");
                    if (vc.getConf().getDbms().equalsIgnoreCase("h2")){
                        StringBuilder conditionExpr = new StringBuilder();
                        // get the reference name of the table in from clause
                        String from = ((ColNameExpr)groupby.get(0)).getTab();
                        for (int i=0;i<groupby.size();i++){
                            // get the table and column name to translate from window function to correlated query
                            String col = ((ColNameExpr)groupby.get(i)).getCol();
                            String tab = ((ColNameExpr)groupby.get(i)).getTab();
                            conditionExpr.append(String.format(" tmp_%s.%s=%s.%s", tab, col, tab, col));
                            if (i!=groupby.size()-1){
                                conditionExpr.append(" AND ");
                            }
                        }
                        if (csql.length()>0) {
                            conditionExpr.append(" AND " + csql);
                        }

                        if (conditionExpr.length() > 0) {
                            conditionExpr.insert(0, " where");
                        }
                        String subquery = String.format("(SELECT count(1) from %s%s) ",
                                UNKNOWN_TABLE_FOR_WINDOW_FUNC, conditionExpr);
                        scaled = BinaryOpExpr.from(vc, scaled, ConstantExpr.from(vc, subquery), "*");
                    }
                    else {
                        scaled = BinaryOpExpr.from(vc, scaled,
                                new FuncExpr(FuncExpr.FuncName.SUM, FuncExpr.count(), new OverClause(groupby)), "*");
                    }
                    return scaled;
                } else if (f.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
                    if (source.sampleType().contains("universe")) {
                        Expr est = new FuncExpr(FuncExpr.FuncName.COUNT_DISTINCT, f.getUnaryExpr());
                        // scale with sampling ratio
                        Expr scaled = BinaryOpExpr.from(vc, est, tableSamplingRatioExpr, "/");
                        // scale with partition size
                        scaled = BinaryOpExpr.from(vc, scaled,
                                ConstantExpr.from(vc, vc.getConf().subsamplingPartitionCount()), "*");
                        // est = scaleWithPartitionSize(est, groupby, partitionCol, forErrorEst);
                        return scaled;
                    } else {
                        VerdictLogger.warn(
                                "Universe sample should be built on the column for accurate distinct-count computations.");
                        return f;
                    }
                } else if (f.getFuncName().equals(FuncExpr.FuncName.SUM)) {
                    // scale with sampling probability
                    Expr scaled = FuncExpr.sum(BinaryOpExpr.from(vc, f.getUnaryExpr(), tupleSamplingProbExpr, "/"));
                    // scale with partition size
                    scaled = BinaryOpExpr.from(vc, scaled, FuncExpr.count(), "/");
                    if (vc.getConf().getDbms().equalsIgnoreCase("h2")){
                        StringBuilder conditionExpr = new StringBuilder();
                        for (int i=0;i<groupby.size();i++){
                            // get the table and column name to translate from window function to correlated query
                            String col = ((ColNameExpr)groupby.get(i)).getCol();
                            String tab = ((ColNameExpr)groupby.get(i)).getTab();
                            conditionExpr.append(String.format(" tmp_%s.%s=%s.%s", tab, col, tab, col));
                            if (i!=groupby.size()-1){
                                conditionExpr.append(" AND ");
                            }
                        }
                        if (csql.length()>0) {
                            conditionExpr.append(" AND " + csql);
                        }
                        if (conditionExpr.length() > 0) {
                            conditionExpr.insert(0, " where");
                        }
                        String subquery = String.format("(SELECT count(1) from %s%s) ",
                                UNKNOWN_TABLE_FOR_WINDOW_FUNC, conditionExpr);
                        scaled = BinaryOpExpr.from(vc, scaled, ConstantExpr.from(vc, subquery), "*");
                    }
                    else {
                        scaled = BinaryOpExpr.from(vc, scaled,
                                new FuncExpr(FuncExpr.FuncName.SUM, FuncExpr.count(), new OverClause(groupby)), "*");
                    }
                    return scaled;
                    // Expr est = scaleForSampling(samplingProbExprs);
                    // est = FuncExpr.sum(BinaryOpExpr.from(vc, s.getUnaryExpr(), est, "*"));
                    // est = scaleWithPartitionSize(est, groupby, partitionCol,
                    // tableSamplingRatioExpr);
                    // return est;
                } else if (f.getFuncName().equals(FuncExpr.FuncName.AVG)) {
                    // scale with sampling probability
                    Expr sumEst = FuncExpr.sum(BinaryOpExpr.from(vc, f.getUnaryExpr(), tupleSamplingProbExpr, "/"));
                    Expr countEst = countNotNull(f.getUnaryExpr(),
                            BinaryOpExpr.from(vc, ConstantExpr.from(vc, 1.0), tupleSamplingProbExpr, "/"));
                    Expr scaled = BinaryOpExpr.from(vc, sumEst, countEst, "/");
                    // Expr scale = scaleForSampling(samplingProbExprs);
                    // Expr sumEst = FuncExpr.sum(BinaryOpExpr.from(vc, s.getUnaryExpr(), scale,
                    // "*"));
                    // // this count-est filters out the null expressions.
                    // Expr countEst = countNotNull(s.getUnaryExpr(), scale);
                    return scaled;
                } else { // expected not to be visited
                    return f;
                }
            } else {
                return expr;
            }
        }
    }

    // private Expr transformForSingleFunctionWithPartitionSize(
    // Expr f,
    // final List<Expr> groupby,
    // final ColNameExpr partitionColExpr,
    // final Expr tupleSamplingProbExpr,
    // final Expr tableSamplingRatioExpr) {
    //
    // ExprModifier v = new ExprModifier(vc) {
    //
    // };
    //
    // return v.visit(f);
    // }

    private Expr scaleForSampling(List<Expr> samplingProbCols) {
        Expr scale = ConstantExpr.from(vc, 1.0);
        for (Expr c : samplingProbCols) {
            scale = BinaryOpExpr.from(vc, scale, c, "/");
        }
        return scale;
    }

    private Expr scaleWithPartitionSize(Expr expr, List<Expr> groupby, boolean forErrorEst) {
        Expr scaled = null;

        if (!forErrorEst) { // point estimate
            if (expr.isCountDistinct()) {
                // scale by the partition count. the ratio between average partition size and
                // the sum of them should be
                // almost same as the inverse of the partition count.
                scaled = BinaryOpExpr.from(vc, expr,
                        new FuncExpr(FuncExpr.FuncName.AVG, FuncExpr.count(), new OverClause(groupby)), "/");
                scaled = BinaryOpExpr.from(vc, scaled,
                        new FuncExpr(FuncExpr.FuncName.SUM, FuncExpr.count(), new OverClause(groupby)), "*");
            } else {
                scaled = BinaryOpExpr.from(vc, expr, FuncExpr.count(), "/");
                scaled = BinaryOpExpr.from(vc, scaled,
                        new FuncExpr(FuncExpr.FuncName.SUM, FuncExpr.count(), new OverClause(groupby)), "*");
            }
        } else {
            // for error estimations, we do not exactly scale with the partition size
            // ratios.
            scaled = BinaryOpExpr.from(vc, expr,
                    new FuncExpr(FuncExpr.FuncName.AVG, FuncExpr.count(), new OverClause(groupby)), "/");
            scaled = BinaryOpExpr.from(vc, scaled,
                    new FuncExpr(FuncExpr.FuncName.SUM, FuncExpr.count(), new OverClause(groupby)), "*");
        }

        return scaled;
    }

    private Expr transformForSingleFunction(Expr f) {
        final Map<TableUniqueName, String> sub = source.tableSubstitution();

        ExprModifier v = new ExprModifier(vc) {
            public Expr call(Expr expr) {
                if (expr instanceof FuncExpr) {
                    // Take two different approaches:
                    // 1. stratified samples: use the verdict_sampling_prob column (in each tuple).
                    // 2. other samples: use either the uniform sampling probability or the ratio
                    // between the sample
                    // size and the original table size.

                    FuncExpr f = (FuncExpr) expr;
                    FuncExpr s = (FuncExpr) exprWithTableNamesSubstituted(expr, sub);
                    List<Expr> samplingProbExprs = source.samplingProbabilityExprsFor(f);

                    if (f.getFuncName().equals(FuncExpr.FuncName.COUNT)) {
                        Expr scale = scaleForSampling(samplingProbExprs);
                        Expr est = FuncExpr.sum(scale);
                        return FuncExpr.round(est);
                    } else if (f.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
                        String dbname = vc.getDbms().getName();
                        Expr scale = scaleForSampling(samplingProbExprs);
                        if (dbname.equals("impala")) {
                            return FuncExpr.round(BinaryOpExpr.from(vc,
                                    new FuncExpr(FuncExpr.FuncName.IMPALA_APPROX_COUNT_DISTINCT, s.getUnaryExpr()),
                                    scale, "*"));
                        } else {
                            return FuncExpr.round(BinaryOpExpr.from(vc, s, scale, "*"));
                        }
                    } else if (f.getFuncName().equals(FuncExpr.FuncName.SUM)) {
                        Expr scale = scaleForSampling(samplingProbExprs);
                        Expr est = FuncExpr.sum(BinaryOpExpr.from(vc, s.getUnaryExpr(), scale, "*"));
                        return est;
                    } else if (f.getFuncName().equals(FuncExpr.FuncName.AVG)) {
                        Expr scale = scaleForSampling(samplingProbExprs);
                        Expr sumEst = FuncExpr.sum(BinaryOpExpr.from(vc, s.getUnaryExpr(), scale, "*"));
                        // this count-est filters out the null expressions.
                        Expr countEst = countNotNull(s.getUnaryExpr(), scale);
                        return BinaryOpExpr.from(vc, sumEst, countEst, "/");
                    } else { // expected not to be visited
                        return s;
                    }
                } else {
                    return expr;
                }
            }
        };

        return v.visit(f);
    }

    private FuncExpr countNotNull(Expr nullcheck, Expr expr) {
        return FuncExpr.sum(new CaseExpr(vc, Arrays.<Cond>asList(new IsCond(nullcheck, new NullCond())),
                Arrays.<Expr>asList(ConstantExpr.from(vc, "0"), expr)));
    }

    @Override
    public double cost() {
        return source.cost();
    }

    @Override
    public String sampleType() {
        return source.sampleType();
    }

    private List<String> mappedSourceSampleColumn(List<String> sourceSampleCols) {
        List<String> cols = new ArrayList<String>();
        for (SelectElem elem : getElemList()) {
            Expr e = elem.getExpr();
            if (!(e instanceof ColNameExpr))
                continue;
            if (sourceSampleCols.contains(((ColNameExpr) e).getCol())) {
                if (elem.aliasPresent()) {
                    cols.add(elem.getAlias());
                } else {
                    cols.add(((ColNameExpr) e).getCol());
                }
            }
        }
        return cols;
    }

    @Override
    protected List<String> getColumnsOnWhichSamplesAreCreated() {
        if (!(source instanceof ApproxGroupedRelation)) {
            return Collections.emptyList();
        } else if (sampleType().equals("stratified") || sampleType().equals("universe")) {
            List<String> cols = mappedSourceSampleColumn(source.getColumnsOnWhichSamplesAreCreated());
            return cols;
        }

        return source.getColumnsOnWhichSamplesAreCreated();
    }

    @Override
    protected String toStringWithIndent(String indent) {
        StringBuilder s = new StringBuilder(1000);
        s.append(indent);
        s.append(String.format("%s(%s) [%s] type: %s (%s), cost: %f\n", this.getClass().getSimpleName(), getAlias(),
                Joiner.on(", ").join(elems), sampleType(), getColumnsOnWhichSamplesAreCreated(), cost()));
        s.append(source.toStringWithIndent(indent + "  "));
        return s.toString();
    }

    @Override
    public boolean equals(ApproxRelation o) {
        if (o instanceof ApproxAggregatedRelation) {
            if (source.equals(((ApproxAggregatedRelation) o).source)) {
                if (elems.equals(((ApproxAggregatedRelation) o).elems)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Sampling probability defers based on the sample type and the preceding
     * groupby. For many cases, sampling probabilities are not easily computable, in
     * which case, we set the return value to zero, so that the sample is not used
     * for computing another aggregation.
     */
    @Override
    public double samplingProbability() {
        String sourceSampleType = source.sampleType();

        if (sourceSampleType.equals("nosample")) {
            return 1.0;
        }

        if (source instanceof ApproxGroupedRelation) {
            List<Expr> groupby = ((ApproxGroupedRelation) source).getGroupby();
            List<String> groupbyInString = new ArrayList<String>();
            for (Expr expr : groupby) {
                if (expr instanceof ColNameExpr) {
                    groupbyInString.add(((ColNameExpr) expr).getCol());
                }
            }

            if (sourceSampleType.equals("universe")) {
                return source.samplingProbability();
            }
        } else {
            return 1.0;
        }

        return 0;
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

    @Override
    public List<ColNameExpr> getAssociatedColumnNames(TableNameExpr tabExpr) {
        List<ColNameExpr> colnames = new ArrayList<ColNameExpr>();
        if (tabExpr == null || tabExpr.getTable().equals(getAlias())) {
            for (SelectElem e : elems) {
                colnames.add(new ColNameExpr(vc, e.getAlias(), getAlias()));
            }
        }
        return colnames;
    }

}
