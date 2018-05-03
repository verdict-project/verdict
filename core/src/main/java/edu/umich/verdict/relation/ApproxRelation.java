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
import java.util.List;
import java.util.Map;

import com.google.common.base.Optional;
import edu.umich.verdict.relation.condition.AndCond;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.*;
import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

/**
 * ApproxRelation indicates what samples should be used for computing the answer
 * to the original query. ApproxRelation includes some helper functions for
 * retrieving sample-related information.
 * 
 * @author Yongjoo Park
 *
 */
public abstract class ApproxRelation extends Relation {

    protected final String partitionSizeAlias = "__vpsize";

    protected ExactRelation original;

    public ApproxRelation(VerdictContext vc) {
        super(vc);
        approximate = true;
        original = null;
    }

    protected void setOriginalRelation(ExactRelation r) {
        original = r;
    }

    protected ExactRelation getOriginalRelation() {
        return original;
    }

    public String sourceTableName() {
        if (this instanceof ApproxSingleRelation) {
            ApproxSingleRelation r = (ApproxSingleRelation) this;
            if (r.getAlias() != null) {
                return r.getAlias();
            } else {
                return r.getSampleName().getTableName();
            }
        } else {
            return this.getAlias();
        }
    }

    /*
     * Aggregations
     */

    public ApproxGroupedRelation groupby(String group) {
        String[] tokens = group.split(",");
        return groupby(Arrays.asList(tokens));
    }

    public ApproxGroupedRelation groupby(List<String> group_list) {
        List<Expr> groups = new ArrayList<Expr>();
        for (String t : group_list) {
            groups.add(Expr.from(vc, t));
        }
        return new ApproxGroupedRelation(vc, this, groups);
    }

    /*
     * Approx
     */

    public ApproxAggregatedRelation agg(Object... elems) {
        return agg(Arrays.asList(elems));
    }

    public ApproxAggregatedRelation agg(List<Object> elems) {
        List<SelectElem> se = new ArrayList<SelectElem>();

        // first insert possible groupby list
        if (this instanceof ApproxGroupedRelation) {
            List<Expr> groupby = ((ApproxGroupedRelation) this).getGroupby();
            for (Expr g : groupby) {
                se.add(new SelectElem(vc, g));
            }
        }

        // now insert aggregation list
        for (Object e : elems) {
            se.add(SelectElem.from(vc, e.toString()));
        }
        return new ApproxAggregatedRelation(vc, this, se);
    }

    @Override
    public ApproxAggregatedRelation count() throws VerdictException {
        return agg(FuncExpr.count());
    }

    @Override
    public ApproxAggregatedRelation sum(String expr) throws VerdictException {
        return agg(FuncExpr.sum(Expr.from(vc, expr)));
    }

    @Override
    public ApproxAggregatedRelation avg(String expr) throws VerdictException {
        return agg(FuncExpr.avg(Expr.from(vc, expr)));
    }

    @Override
    public ApproxAggregatedRelation countDistinct(String expr) throws VerdictException {
        return agg(FuncExpr.countDistinct(Expr.from(vc, expr)));
    }

    /**
     * Properly scale all aggregation functions so that the final answers are
     * correct. For ApproxAggregatedRelation: returns a AggregatedRelation instance
     * whose result is approximately correct. For ApproxSingleRelation,
     * ApproxJoinedRelation, and ApproxFilteredRelaation: returns a select statement
     * from sample tables. The rewritten sql doesn't have much meaning if not used
     * by ApproxAggregatedRelation.
     * 
     * @return
     */
    public ExactRelation rewrite() {
        if (vc.getConf().errorBoundMethod().equals("nobound")) {
            return rewriteForPointEstimate();
        } else if (vc.getConf().errorBoundMethod().equals("subsampling")) {
            return rewriteWithSubsampledErrorBounds();
        } else if (vc.getConf().errorBoundMethod().equals("bootstrapping")) {
            return rewriteWithBootstrappedErrorBounds();
        } else {
            VerdictLogger.error(this,
                    "Unsupported error bound computation method: " + vc.getConf().get("verdict.error_bound_method"));
            return null;
        }
    }

    public abstract ExactRelation rewriteForPointEstimate();

    /**
     * Creates an exact relation that computes approximate aggregates and their
     * error bounds using subsampling.
     * 
     * If a sample plan does not include any sample tables, it will simply be exact
     * answers; thus, no error bounds are necessary.
     * 
     * If a sample plan includes at least one sample table in the table sources,
     * there must exists a partition column (__vpart by default).
     * {@link ExactRelation#partitionColumn()} finds and returns an appropriate
     * column name for a given source relation. See the method for details on how
     * the partition column of a table is determined. Note that the partition
     * columns are defined for rewritten relations.
     * 
     * @return
     */
    public ExactRelation rewriteWithSubsampledErrorBounds() {
        VerdictLogger.error(this,
                String.format("Calling a method, %s, on unappropriate class", "rewriteWithSubsampledErrorBounds()"));
        return null;
    }

    /**
     * Internal method for
     * {@link ApproxRelation#rewriteWithSubsampledErrorBounds()}.
     * 
     * @return
     */
    protected abstract ExactRelation rewriteWithPartition();

    // These functions are moved to ExactRelation
    // This is because partition column name could be only properly resolved after
    // the rewriting to the
    // exact relation is finished.
    // protected String partitionColumnName() {
    // return vc.getDbms().partitionColumnName();
    // }
    // returns effective partition column name for a possibly joined table.
    // protected abstract ColNameExpr partitionColumn();

    public ExactRelation rewriteWithBootstrappedErrorBounds() {
        return null;
    }

    /**
     * Computes an appropriate sampling probability for a particular aggregate
     * function. For uniform random sample, returns the ratio between the sample
     * table and the original table. For universe sample, if the aggregate function
     * is COUNT, AVG, SUM, returns the ratio between the sample table and the
     * original table. if the aggregate function is COUNT-DISTINCT, returns the
     * sampling probability. For stratified sample, this method returns the sampling
     * probability only for the joined tables.
     * 
     * Verdict sample rules.
     * 
     * For COUNT, AVG, and SUM, uniform random samples, universe samples, stratified
     * samples, or no samples can be used. For COUNT-DISTINCT, universe sample,
     * stratified samples, or no samples can be used. For stratified samples, the
     * distinct number of groups is assumed to be limited.
     * 
     * Verdict join rules.
     * 
     * (uniform, uniform) -> uniform (uniform, stratified) -> stratified (uniform,
     * universe) -> uniform (uniform, no sample) -> uniform (stratified, stratified)
     * -> stratified (stratified, universe) -> no allowed (stratified, no sample) ->
     * stratified (universe, universe) -> universe (only when the columns on which
     * samples are built coincide) (universe, no sample) -> universe
     * 
     * @param f
     * @return
     */
    @Deprecated
    protected abstract List<Expr> samplingProbabilityExprsFor(FuncExpr f);

    /**
     * rough sampling probability, which is obtained from the sampling params.
     * 
     * @return
     */
    public abstract double samplingProbability();

    public abstract double cost();

    /**
     * The returned contains the tuple-level sampling probability. For universe and
     * uniform samples, this is basically the ratio of the sample size to the
     * original table size.
     * 
     * @return
     */
    public abstract Expr tupleProbabilityColumn();

    /**
     * The returned column contains
     * 
     * @return
     */
    public abstract Expr tableSamplingRatio();

    /**
     * Returns an effective sample type of this relation.
     * 
     * @return One of "uniform", "universe", "stratified", "nosample".
     */
    public abstract String sampleType();

    // protected abstract List<ColNameExpr> accumulateSamplingProbColumns();

    /**
     * Returns a set of columns on which a sample is created. Only meaningful for
     * stratified and universe samples.
     * 
     * @return
     */
    protected abstract List<String> getColumnsOnWhichSamplesAreCreated();

    /**
     * Pairs of original table name and a sample table name. This function does not
     * inspect subqueries. The substitution expression is an alias (thus, string
     * type).
     * 
     * @return
     */
    protected abstract Map<TableUniqueName, String> tableSubstitution();

    /**
     * Returns true if any of the table sources include an non-nosample relation.
     * 
     * @return
     */
    protected abstract boolean doesIncludeSample();

    /*
     * order by and limit
     */

    public ApproxRelation orderby(String orderby) {
        String[] tokens = orderby.split(",");
        List<OrderByExpr> o = new ArrayList<OrderByExpr>();
        for (String t : tokens) {
            o.add(OrderByExpr.from(vc, t));
        }
        return new ApproxOrderedRelation(vc, this, o);
    }

    public ApproxRelation limit(long limit) {
        return new ApproxLimitedRelation(vc, this, limit);
    }

    /*
     * sql
     */

    @Override
    public String toSql() {
        ExactRelation r = rewrite();
        return r.toSql();
    }

    @Override
    public String toString() {
        return toStringWithIndent("");
    }

    public abstract boolean equals(ApproxRelation o);

    protected abstract String toStringWithIndent(String indent);

    /*
     * Helpers
     */

    protected double confidenceIntervalMultiplier() {
        double confidencePercentage = vc.getConf().errorBoundConfidenceInPercentage();
        if (confidencePercentage == 0.999) {
            return 3.291;
        } else if (confidencePercentage == 0.995) {
            return 2.807;
        } else if (confidencePercentage == 0.99) {
            return 2.576;
        } else if (confidencePercentage == 0.95) {
            return 1.96;
        } else if (confidencePercentage == 0.90) {
            return 1.645;
        } else if (confidencePercentage == 0.85) {
            return 1.44;
        } else if (confidencePercentage == 0.80) {
            return 1.282;
        } else {
            VerdictLogger.warn(this,
                    String.format("Unsupported confidence: %s%%. Uses the default 95%%.", confidencePercentage * 100));
            return 1.96; // 95% by default.
        }
    }

    protected Expr exprWithTableNamesSubstituted(Expr expr, Map<TableUniqueName, String> sub) {
        TableNameReplacerInExpr v = new TableNameReplacerInExpr(vc, sub);
        return v.visit(expr);
    }

    protected static Pair<List<Expr>, ApproxRelation> allPrecedingGroupbys(ApproxRelation r) {
        List<Expr> groupbys = new ArrayList<Expr>();
        ApproxRelation t = r;
        while (true) {
            if (t instanceof ApproxGroupedRelation) {
                groupbys.addAll(((ApproxGroupedRelation) t).getGroupby());
                t = ((ApproxGroupedRelation) t).getSource();
            } else {
                break;
            }
        }
        return Pair.of(groupbys, t);
    }

    //copy from ExactRelation; Use for obtaining From table in ApproxAggregatedRelation
    protected Pair<Optional<Cond>, ApproxRelation> allPrecedingFilters(ApproxRelation r) {
        Optional<Cond> c = Optional.absent();
        ApproxRelation t = r;
        while (true) {
            if (t instanceof ApproxFilteredRelation) {
                if (c.isPresent()) {
                    c = Optional.of((Cond) AndCond.from(c.get(), ((ApproxFilteredRelation) t).getFilter()));
                } else {
                    c = Optional.of(((ApproxFilteredRelation) t).getFilter());
                }
                t = ((ApproxFilteredRelation) t).getSource();
            } else {
                break;
            }
        }
        return Pair.of(c, t);
    }

    //directly copy from ExactRelation
    protected String sourceExpr(ApproxRelation source) {
        if (source instanceof ApproxSingleRelation) {
            ApproxSingleRelation asource = (ApproxSingleRelation) source;
            TableUniqueName tableName = asource.getTableName();
            String alias = asource.getAlias();
            return String.format("%s AS %s", tableName, alias);
        } else if (source instanceof ApproxJoinedRelation) {
            return ((ApproxJoinedRelation) source).joinClause();
        } else if (source instanceof ApproxLateralViewRelation) {
            ApproxLateralViewRelation lv = (ApproxLateralViewRelation) source;
            LateralFunc func = lv.getLateralFunc();
            return String.format("%s %s AS %s", func.toSql(), lv.getTableAlias(), lv.getColumnAlias());
        } else {
            String alias = source.getAlias();
            if (alias == null) {
                alias = Relation.genTableAlias();
            }
            return String.format("(%s) AS %s", source.toSql(), alias);
        }
    }

    /**
     * @param tabExpr Restricted to this table.
     * @return A list of all column names.
     */
    abstract public List<ColNameExpr> getAssociatedColumnNames(TableNameExpr tabExpr);
}
