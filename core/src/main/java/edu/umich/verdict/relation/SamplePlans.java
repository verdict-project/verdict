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
import java.util.Comparator;
import java.util.List;

public class SamplePlans {

    private List<SamplePlan> plans;

    public SamplePlans() {
        this(new ArrayList<SamplePlan>());
    }

    public SamplePlans(List<SamplePlan> plans) {
        this.plans = plans;
    }

    public void add(SamplePlan plan) {
        plans.add(plan);
    }

    public List<SamplePlan> getPlans() {
        return plans;
    }

    /**
     * Creates candidate plans that can answer one more expression.
     * 
     * @param groups
     *            A candidate approx relations that can answer an expression
     */
    public void consolidateNewExpr(List<SampleGroup> groups) {
        List<SamplePlan> newPlans = new ArrayList<SamplePlan>();

        if (plans.size() == 0) {
            for (SampleGroup group : groups) {
                newPlans.add(new SamplePlan(Arrays.asList(group)));
            }
        } else {
            for (SamplePlan oldPlan : plans) {
                for (SampleGroup group : groups) {
                    newPlans.add(oldPlan.createByMerge(group));
                }
            }
        }
        this.plans = newPlans;
        prune(10);
    }

    // keeps the top 'num' number of plans.
    // this method does not remove the plan that includes only a single sample
    // group.
    public void prune(int num) {

        Comparator<SamplePlan> costComparator = new Comparator<SamplePlan>() {
            @Override
            public int compare(SamplePlan a, SamplePlan b) {
                if (a.cost() < b.cost()) {
                    return -1;
                } else if (a.cost() > b.cost()) {
                    return 1;
                } else {
                    return 0;
                }
            }
        };

        Collections.sort(plans, costComparator);

        List<SamplePlan> newPlans = new ArrayList<SamplePlan>();

        for (int i = 0; i < plans.size(); i++) {
            if (num > 0) {
                newPlans.add(plans.get(i));
                num--;
                continue;
            }

            if (plans.get(i).getSampleGroups().size() == 1) {
                newPlans.add(plans.get(i));
            }
        }

        plans = newPlans;
    }

    public SamplePlan bestPlan(double relative_cost_ratio) {
        // find the original plan (the plan that uses the original tables) and the cost
        // of the plan.
        double original_cost = Double.NEGATIVE_INFINITY;
        SamplePlan original_plan = null;
        for (SamplePlan plan : plans) {
            if (plan.getSampleGroups().size() == 1) {
                double cost = plan.cost();
                if (cost > original_cost) {
                    original_cost = cost;
                    original_plan = plan;
                }
            }
        }

        // find the best plan (the plan whose cost is not too large and its sampling
        // prob is good)
        SamplePlan best = null;
        double bestScore = Double.NEGATIVE_INFINITY;

        SamplePlan fallback = null;
        double fallbackCost = Double.POSITIVE_INFINITY;

        for (SamplePlan plan : plans) {
            double cost = plan.cost();
            if (cost < original_cost * relative_cost_ratio) {
                double samplingProb = plan.harmonicSamplingProb();
                double thisScore = computeScore(cost, samplingProb);
                if (thisScore > bestScore) {
                    bestScore = thisScore;
                    best = plan;
                }
                // VerdictLogger.debug(this, plan.toString() + ", cost: " + cost + ", prob: " +
                // plan.harmonicSamplingProb());
            }

            if (cost < fallbackCost) {
                fallbackCost = cost;
                fallback = plan;
            }
        }

        if (best != null) {
            return best;
        } else {
            return fallback;
        }
    }

    private double computeScore(double cost, double samplingProbability) {
        return samplingProbability / Math.sqrt(cost);
    }

}
