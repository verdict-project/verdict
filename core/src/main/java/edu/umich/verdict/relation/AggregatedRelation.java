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

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.SelectElem;
import edu.umich.verdict.util.VerdictLogger;

/**
 * Represents aggregation operations on any source relation. This relation is
 * expected to be a child of a ProjectedRelation instance (which is always
 * ensured when this instance is created from a sql statement).
 * 
 * @author Yongjoo Park
 * 
 *         This class provides extended operations than ProjectedRelation
 *
 */
public class AggregatedRelation extends ExactRelation {

    protected ExactRelation source;

    protected List<SelectElem> elems;

    protected AggregatedRelation(VerdictContext vc, ExactRelation source, List<SelectElem> elems) {
        super(vc);
        this.source = source;
        this.elems = elems;
        subquery = true;
    }

    // Copy constructor.
    public AggregatedRelation(AggregatedRelation other) {
        super(other.vc);
        this.name = other.name;
        this.source = other.source;
        this.elems = other.elems;
        this.subquery = true;
    }

    @Override
    protected String getSourceName() {
        return getAlias();
    }

    public ExactRelation getSource() {
        return source;
    }

    public List<SelectElem> getElemList() {
        return elems;
    }

    /*
     * Approx
     */

    /**
     * if the source is a grouped relation, only a few sample types are allowed as a
     * source of another aggregate relation. 1. stratified sample: the groupby
     * column must be equal to the columns on which samples were built on. the
     * sample type of the aggregated relation will be "nosample" and the sample type
     * will be "1.0". 2. universe sample: the groupby column must be equal to the
     * columns on which samples where built on. the sample type of the aggregated
     * relation will be "universe" sampled on the same columns. the sampling
     * probability will also stays the same.
     */
    @Override
    protected List<ApproxRelation> nBestSamples(Expr elem, int n) throws VerdictException {
        SamplePlans plans = candidatesAsRoot();
        List<ApproxRelation> candidates = new ArrayList<ApproxRelation>();
        List<FuncExpr> funcs = elem.extractFuncExpr();
        for (SamplePlan plan : plans.getPlans()) {
            ApproxRelation a = plan.toRelation(vc, getAlias());
            boolean isEligible = true;

            for (FuncExpr fexpr : funcs) {
                if (fexpr.getFuncName().equals(FuncExpr.FuncName.COUNT)
                        || fexpr.getFuncName().equals(FuncExpr.FuncName.AVG)
                        || fexpr.getFuncName().equals(FuncExpr.FuncName.SUM)
                        || fexpr.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
                    if (source instanceof GroupedRelation) {
                        if (a.sampleType().equals("universe") || a.sampleType().equals("stratified")
                                || a.sampleType().equals("nosample")) {
                        } else {
                            isEligible = false;
                        }
                    }
                } else {
                    // for all other functions, we don't perform any approximations.
                    // this was introduced to handle tpch q15; we need a better mechanism.
                }
            }

            if (isEligible) {
                candidates.add(a);
            }
        }

        return candidates;
    }

    private SamplePlans candidatesAsRoot() throws VerdictException {
        // these are candidates for the sources of this relation
        List<List<SampleGroup>> candidates_list = new ArrayList<List<SampleGroup>>();

        for (int i = 0; i < elems.size(); i++) {
            SelectElem elem = elems.get(i);
            Expr agg = elem.getExpr();

            // TODO: make this number (10) configurable.
            List<ApproxRelation> candidates = source.nBestSamples(agg, 10);
            List<SampleGroup> sampleGroups = new ArrayList<SampleGroup>();
            for (ApproxRelation a : candidates) {
                sampleGroups.add(new SampleGroup(a, Arrays.asList(elem)));
            }
            candidates_list.add(sampleGroups);
        }

        // We test if we can consolidate those sample candidates so that the number of
        // select statements is less than
        // the number of the expressions. In the worst case (e.g., all count-distinct),
        // the number of select statements
        // will be equal to the number of the expressions. If the cost of running those
        // select statements individually
        // is higher than the cost of running a single select statement using the
        // original tables, samples are not used.
        SamplePlans consolidatedPlans = consolidate(candidates_list);
        return consolidatedPlans;
    }

    public ApproxRelation approx() throws VerdictException {
        SamplePlans consolidatedPlans = candidatesAsRoot();
        SamplePlan plan = chooseBestPlan(consolidatedPlans);

        if (plan == null) {
            String msg = "No feasible sample plan is found.";
            VerdictLogger.error(this, msg);
            throw new VerdictException(msg);
        }
        VerdictLogger.debug(this, "The sample plan to use: ");
        VerdictLogger.debugPretty(this, plan.toPrettyString(), "  ");

        ApproxRelation r = plan.toRelation(vc, getAlias());
        r.setOriginalRelation(this);
        return r;
    }

    public ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
        ApproxRelation a = new ApproxAggregatedRelation(vc, source.approxWith(replace), elems);
        a.setAlias(getAlias());
        a.setOriginalRelation(this);
        return a;
    }

    /*
     * Sql
     */

    protected String selectSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");

        sql.append(Joiner.on(", ").join(elems));
        return sql.toString();
    }

    @Deprecated
    protected String withoutSelectSql() {
        StringBuilder sql = new StringBuilder();

        Pair<List<Expr>, ExactRelation> groupsAndNextR = allPrecedingGroupbys(this.source);
        List<Expr> groupby = groupsAndNextR.getLeft();

        Pair<Optional<Cond>, ExactRelation> filtersAndNextR = allPrecedingFilters(groupsAndNextR.getRight());
        String csql = (filtersAndNextR.getLeft().isPresent()) ? filtersAndNextR.getLeft().get().toString() : "";

        sql.append(String.format(" FROM %s", sourceExpr(filtersAndNextR.getRight())));
        if (csql.length() > 0) {
            sql.append(" WHERE ");
            sql.append(csql);
        }
        if (groupby.size() > 0) {
            sql.append(" GROUP BY ");
            sql.append(Joiner.on(", ").join(groupby));
        }
        return sql.toString();
    }

    public String toSql() {
        StringBuilder sql = new StringBuilder();

        Pair<List<Expr>, ExactRelation> groupsAndNextR = allPrecedingGroupbys(this.source);
        List<Expr> groupby = groupsAndNextR.getLeft();
        Pair<Optional<Cond>, ExactRelation> filtersAndNextR = allPrecedingFilters(groupsAndNextR.getRight());
        String csql = (filtersAndNextR.getLeft().isPresent()) ? filtersAndNextR.getLeft().get().toString() : "";

        sql.append(selectSql());
        sql.append(String.format(" FROM %s", sourceExpr(filtersAndNextR.getRight())));
        String new_sql = sql.toString();
        String sql_with_temp = sourceExprWithTempAlias(filtersAndNextR.getRight());
        // get table name of from clause; replace the unknown table name
        new_sql = new_sql.replace(UNKNOWN_TABLE_FOR_WINDOW_FUNC, sql_with_temp);
        sql.replace(0,sql.length(), new_sql);
//        if (idx != -1){
//            new_sql = new_sql.replace(UNKNOWN_TABLE_FOR_WINDOW_FUNC, sourceExpr(filtersAndNextR.getRight()).substring(0,idx));
//            sql.replace(0,sql.length(), new_sql);
//        } else {
//            new_sql = new_sql.replace(UNKNOWN_TABLE_FOR_WINDOW_FUNC, sourceExpr(filtersAndNextR.getRight()).substring(0,idx));
//            sql.replace(0,sql.length(), new_sql);
//        }

        if (csql.length() > 0) {
            sql.append(" WHERE ");
            sql.append(csql);
        }
        if (groupby.size() > 0) {
            sql.append(" GROUP BY ");
            sql.append(Joiner.on(", ").join(groupby));
        }
        return sql.toString();
    }

    @Override
    public List<ColNameExpr> accumulateSamplingProbColumns() {
        ColNameExpr expr = new ColNameExpr(vc, samplingProbabilityColumnName(), getAlias());
        return Arrays.asList(expr);
    }

    @Override
    protected String toStringWithIndent(String indent) {
        StringBuilder s = new StringBuilder(1000);
        s.append(indent);
        s.append(String.format("%s(%s) [%s]\n", this.getClass().getSimpleName(), getAlias(),
                Joiner.on(", ").join(elems)));
        s.append(source.toStringWithIndent(indent + "  "));
        return s.toString();
    }

    @Override
    public ColNameExpr partitionColumn() {
        ColNameExpr col = new ColNameExpr(vc, partitionColumnName(), getAlias());
        return col;
    }

    @Override
    public List<SelectElem> getSelectElemList() {
        return elems;
    }

}
