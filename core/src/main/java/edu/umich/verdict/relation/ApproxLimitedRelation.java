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

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.TableNameExpr;
import edu.umich.verdict.util.VerdictLogger;

public class ApproxLimitedRelation extends ApproxRelation {

    private ApproxRelation source;

    private long limit;

    public ApproxLimitedRelation(VerdictContext vc, ApproxRelation source, long limit) {
        super(vc);
        this.source = source;
        this.limit = limit;
        this.alias = source.alias;
    }

    @Override
    public ExactRelation rewriteForPointEstimate() {
        ExactRelation r = new LimitedRelation(vc, source.rewriteForPointEstimate(), limit);
        r.setAlias(getAlias());
        return r;
    }

    @Override
    public ExactRelation rewriteWithSubsampledErrorBounds() {
        ExactRelation r = new LimitedRelation(vc, source.rewriteWithSubsampledErrorBounds(), limit);
        r.setAlias(getAlias());
        return r;
    }

    @Override
    public ExactRelation rewriteWithPartition() {
        ExactRelation r = new LimitedRelation(vc, source.rewriteWithPartition(), limit);
        r.setAlias(getAlias());
        return r;
    }

    @Override
    protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
        return source.samplingProbabilityExprsFor(f);
    }

    @Override
    protected Map<TableUniqueName, String> tableSubstitution() {
        return ImmutableMap.of();
    }

    @Override
    public String sampleType() {
        return null;
    }

    @Override
    public double cost() {
        return source.cost();
    }

    @Override
    protected List<String> getColumnsOnWhichSamplesAreCreated() {
        return null;
    }

    @Override
    protected String toStringWithIndent(String indent) {
        StringBuilder s = new StringBuilder(1000);
        s.append(indent);
        s.append(String.format("%s(%s) [%d]\n", this.getClass().getSimpleName(), getAlias(), limit));
        s.append(source.toStringWithIndent(indent + "  "));
        return s.toString();
    }

    @Override
    public boolean equals(ApproxRelation o) {
        if (o instanceof ApproxLimitedRelation) {
            if (source.equals(((ApproxLimitedRelation) o).source)) {
                if (limit == ((ApproxLimitedRelation) o).limit) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public double samplingProbability() {
        VerdictLogger.warn(this, "sampling probability on LimitedRelation is meaningless.");
        return 0;
    }

    @Override
    protected boolean doesIncludeSample() {
        return source.doesIncludeSample();
    }

    @Override
    public Expr tupleProbabilityColumn() {
        return null;
    }

    @Override
    public Expr tableSamplingRatio() {
        return null;
    }

    @Override
    public List<ColNameExpr> getAssociatedColumnNames(TableNameExpr tabExpr) {
        return source.getAssociatedColumnNames(tabExpr);
    }

}
