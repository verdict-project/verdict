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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.TableNameExpr;

public class ApproxGroupedRelation extends ApproxRelation {

    private ApproxRelation source;

    private List<Expr> groupby;

    public ApproxGroupedRelation(VerdictContext vc, ApproxRelation source, List<Expr> groupby) {
        super(vc);
        this.source = source;
        this.groupby = groupby;
        this.alias = source.alias;
    }

    public ApproxRelation getSource() {
        return source;
    }

    public List<Expr> getGroupby() {
        return groupby;
    }

    @Override
    public ExactRelation rewriteForPointEstimate() {
        List<Expr> newGroupby = groupbyWithTablesSubstituted();
        ExactRelation r = new GroupedRelation(vc, source.rewriteForPointEstimate(), newGroupby);
        r.setAlias(r.getAlias());
        return r;
    }

    @Override
    public ExactRelation rewriteWithPartition() {
        ExactRelation newSource = source.rewriteWithPartition();
        List<Expr> newGroupby = groupbyWithTablesSubstituted();
        // newGroupby.add((ColNameExpr) exprWithTableNamesSubstituted(partitionColumn(),
        // tableSubstitution()));
        ColNameExpr partitionCol = newSource.partitionColumn();
        if (partitionCol != null) {
//            // if the newSource is a subquery, the table alias of partitionCol must change.
//        	if (newSource instanceof ProjectedRelation || newSource instanceof AggregatedRelation) {
//        		partitionCol.setTab(getAlias());
//        	}
            newGroupby.add(partitionCol);
        }
        ExactRelation r = new GroupedRelation(vc, newSource, newGroupby);
        r.setAlias(r.getAlias());
        return r;
    }

    // @Override
    // protected ColNameExpr partitionColumn() {
    // return source.partitionColumn();
    // }

    @Override
    // TODO: make this more accurate for handling IN and EXISTS predicates.
    protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
        return source.samplingProbabilityExprsFor(f);
    }

    @Override
    protected Map<TableUniqueName, String> tableSubstitution() {
        return source.tableSubstitution();
    }

    protected List<Expr> groupbyWithTablesSubstituted() {
        Map<TableUniqueName, String> sub = tableSubstitution();
        List<Expr> replaced = new ArrayList<Expr>();
        for (Expr e : groupby) {
            replaced.add(exprWithTableNamesSubstituted(e, sub));
        }
        return replaced;
    }

    private Set<String> columnNamesInGroupby() {
        List<Expr> groupby = getGroupby();
        Set<String> strGroupby = new HashSet<String>();
        for (Expr expr : groupby) {
            strGroupby.addAll(expr.extractColNames());
        }
        return strGroupby;
    }

    @Override
    public String sampleType() {
        String sampleType = source.sampleType();
        if (sampleType.equals("nosample"))
            return "nosample";

        Set<String> groupbyStr = columnNamesInGroupby();
        Set<String> sampleColumns = new HashSet<String>(source.getColumnsOnWhichSamplesAreCreated());

        if (sampleType.equals("universe") && groupbyStr.equals(sampleColumns)) {
            return "universe";
        } else if (sampleType.equals("stratified") && groupbyStr.equals(sampleColumns)) {
            return "nosample";
        }

        return "grouped-" + sampleType;
    }

    @Override
    public double cost() {
        return source.cost();
    }

    @Override
    protected List<String> getColumnsOnWhichSamplesAreCreated() {
        return source.getColumnsOnWhichSamplesAreCreated();
    }

    @Override
    protected String toStringWithIndent(String indent) {
        StringBuilder s = new StringBuilder(1000);
        s.append(indent);
        s.append(String.format("%s(%s, %s (%s)) [%s]\n", this.getClass().getSimpleName(), getAlias(), sampleType(),
                getColumnsOnWhichSamplesAreCreated().toString(), Joiner.on(", ").join(groupby)));
        s.append(source.toStringWithIndent(indent + "  "));
        return s.toString();
    }

    @Override
    public boolean equals(ApproxRelation o) {
        if (o instanceof ApproxGroupedRelation) {
            if (source.equals(((ApproxGroupedRelation) o).source)) {
                if (groupby.equals(((ApproxGroupedRelation) o).groupby)) {
                    return true;
                }
            }
        }
        return false;
    }

    // assumes that this method is called by the parent, i.e.,
    // ApproxAggregatedRelation.
    @Override
    public double samplingProbability() {
        Set<String> groupbyStr = columnNamesInGroupby();
        Set<String> sampleColumns = new HashSet<String>(source.getColumnsOnWhichSamplesAreCreated());

        if (sampleColumns.equals(groupbyStr)) {
            if (sampleType().equals("universe")) {
                return Math.min(2 * source.samplingProbability(), 1.0);
            } else if (sampleType().equals("nosample")) {
                return Math.min(5 * source.samplingProbability(), 1.0);
            }
        }

        return source.samplingProbability();
    }

    @Override
    protected boolean doesIncludeSample() {
        return source.doesIncludeSample();
    }

    @Override
    public Expr tupleProbabilityColumn() {
        Expr e = source.tupleProbabilityColumn();
//        e = e.withTableSubstituted(getAlias());   why is this needed?
        return e;
    }

    @Override
    public Expr tableSamplingRatio() {
        return source.tableSamplingRatio();
    }

    @Override
    public List<ColNameExpr> getAssociatedColumnNames(TableNameExpr tabExpr) {
        return source.getAssociatedColumnNames(tabExpr);
    }

}
