/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.SelectElem;

/**
 * Stores information about what samples to use to compute multiple expressions.
 * A single SampleGroup instance stores the mapping from a set of samples to a
 * list of expressions to answer using the set, and this class includes multiple
 * number of such SampleGroup instances so that they can answer the
 * user-submitted query when combined.
 * 
 * @author Yongjoo Park
 */
public class SamplePlan {

    private List<SampleGroup> sampleGroups;

    private Double cachedCost;

    public SamplePlan() {
        this(new ArrayList<SampleGroup>());
    }

    public SamplePlan(List<SampleGroup> sampleGroups) {
        this.sampleGroups = sampleGroups;
        cachedCost = null;
    }

    public SamplePlan duplicate() {
        List<SampleGroup> copy = new ArrayList<SampleGroup>();
        for (SampleGroup g : sampleGroups) {
            copy.add(g.duplicate());
        }
        return new SamplePlan(copy);
    }

    public List<SampleGroup> getSampleGroups() {
        return sampleGroups;
    }

    public List<ApproxRelation> getApproxRelations() {
        List<ApproxRelation> a = new ArrayList<ApproxRelation>();
        for (SampleGroup g : getSampleGroups()) {
            a.add(g.getSample());
        }
        return a;
    }

    @Override
    public String toString() {
        return sampleGroups.toString();
    }

    public String toPrettyString() {
        StringBuilder s = new StringBuilder();
        for (SampleGroup g : sampleGroups) {
            s.append(g.toString());
            s.append("\n");
        }
        return s.toString();
    }

    public double cost() {
        if (cachedCost != null)
            return cachedCost;

        double cost = 0;
        for (SampleGroup g : sampleGroups) {
            cost += g.cost();
        }
        cachedCost = cost;
        return cost;
    }

    /**
     * Computes the harmonic mean of the sampling probabilities of the SampleGroup
     * instances.
     * 
     * @return
     */
    public double harmonicSamplingProb() {
        double samplingProb = 0;
        for (SampleGroup g : sampleGroups) {
            samplingProb += 1 / g.samplingProb();
        }
        return sampleGroups.size() / samplingProb;
    }

    /**
     * creates and returns a new SamplePlan by merging the new sample group.
     * 
     * @param group
     * @return
     */
    public SamplePlan createByMerge(SampleGroup group) {
        SamplePlan copy = this.duplicate();

        // check if there's an existing sample group into which a new sample group can
        // be combined.
        boolean merged = false;
        for (SampleGroup g : copy.sampleGroups) {
            if (g.isEqualSample(group)) {
                g.addElem(group.getElems());
                // ApproxRelation a = (ApproxAggregatedRelation) g.getSample();
                // g.setSample(new ApproxAggregatedRelation(a.getVerdictContext(),
                // a.getSource(), g.getElems()));
                merged = true;
                break;
            }
        }

        if (!merged) {
            copy.sampleGroups.add(group);
        }

        return copy;
    }

    public ApproxRelation toRelation(VerdictContext vc, String alias) {
        List<SampleGroup> aggregateSources = getSampleGroups();

        // Join the results from those multiple relations (if there are more than one)
        // The root (r) will be AggregatedRelation if there is only one relation
        // The root (r) will be a ProjectedRelation of JoinedRelation if there are more
        // than one relation
        ApproxRelation r = new ApproxAggregatedRelation(vc, aggregateSources.get(0).getSample(),
                aggregateSources.get(0).getElems());

        if (aggregateSources.size() > 1) {
            for (int i = 1; i < aggregateSources.size(); i++) {
                ApproxRelation s1 = aggregateSources.get(i).getSample();
                List<SelectElem> elems1 = aggregateSources.get(i).getElems();
                ApproxRelation r1 = new ApproxAggregatedRelation(vc, s1, elems1);

                String ln = r.getAlias();
                String rn = r1.getAlias();

                if (!(s1 instanceof ApproxGroupedRelation)) {
                    r = new ApproxJoinedRelation(vc, r, r1, null);
                } else {
                    List<Expr> groupby = ((ApproxGroupedRelation) s1).getGroupby();
                    List<Pair<Expr, Expr>> joincols = new ArrayList<Pair<Expr, Expr>>();
                    for (Expr col : groupby) {
                        // replace table names in internal colNameExpr
                        joincols.add(Pair.of(col.withTableSubstituted(ln), col.withTableSubstituted(rn)));
                    }
                    r = new ApproxJoinedRelation(vc, r, r1, joincols);
                }
            }
        }

        r.setAlias(alias);
        return r;
    }

}
