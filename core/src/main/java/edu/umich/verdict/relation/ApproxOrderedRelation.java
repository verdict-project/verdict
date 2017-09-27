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

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
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
        this.alias = source.alias;
    }

    @Override
    public ExactRelation rewriteForPointEstimate() {
        ExactRelation r = new OrderedRelation(vc, source.rewriteForPointEstimate(), orderby);
        r.setAlias(getAlias());
        return r;
    }

    @Override
    public ExactRelation rewriteWithSubsampledErrorBounds() {
        ExactRelation newSource = source.rewriteWithSubsampledErrorBounds();
        ExactRelation r = new OrderedRelation(vc, newSource, orderby);
        r.setAlias(getAlias());
        return r;
    }

    @Override
    public ExactRelation rewriteWithPartition() {
        ExactRelation r = new OrderedRelation(vc, source.rewriteWithPartition(), orderby);
        r.setAlias(getAlias());
        return r;
    }

    @Override
    protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
        return source.samplingProbabilityExprsFor(f);
    }

    @Override
    protected Map<TableUniqueName, String> tableSubstitution() {
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
        s.append(String.format("%s(%s) [%s]\n", this.getClass().getSimpleName(), getAlias(),
                Joiner.on(", ").join(orderby)));
        s.append(source.toStringWithIndent(indent + "  "));
        return s.toString();
    }

    @Override
    public boolean equals(ApproxRelation o) {
        if (o instanceof ApproxOrderedRelation) {
            if (source.equals(((ApproxOrderedRelation) o).source)) {
                if (orderby.equals(((ApproxOrderedRelation) o).orderby)) {
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
        return source.tupleProbabilityColumn();
    }

    @Override
    public Expr tableSamplingRatio() {
        return source.tableSamplingRatio();
    }

}
