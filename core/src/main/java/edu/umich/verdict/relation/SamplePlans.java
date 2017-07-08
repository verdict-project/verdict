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
	
	/**
	 * Creates candidate plans that can answer one more expression.
	 * @param plan A plan that includes SampleGroup instances for a single expression.
	 */
	public void consolidateNewExpr(List<SampleGroup> groups) {
		List<SamplePlan> newPlans = new ArrayList<SamplePlan>();
		
		if (plans.size() == 0) {
			for (SampleGroup group : groups) {
				newPlans.add(new SamplePlan(Arrays.asList(group)));
			}
		}
		else {
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
	// this method does not remove the plan that includes only a single sample group.
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
		// find the original plan (the plan that uses the original tables) and the cost of the plan.
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

		// find the best plan (the plan whose cost is not too large and its sampling prob is good)
		SamplePlan best = null;
		double bestScore = Double.NEGATIVE_INFINITY;
		for (SamplePlan plan : plans) {
			double cost = plan.cost();
			if (cost < original_cost * relative_cost_ratio) {
				double samplingProb = plan.harmonicSamplingProb();
				double thisScore = computeScore(cost, samplingProb);
				if (thisScore > bestScore) {
					bestScore = thisScore;
					best = plan;
				}
//				VerdictLogger.debug(this, plan.toString() + ", cost: " + cost + ", prob: " + plan.harmonicSamplingProb());
			}
		}

		if (best != null) {
			return best;
		} else {
			return original_plan;
		}
	}
	
	private double computeScore(double cost, double samplingProbability) {
		return samplingProbability / Math.sqrt(cost);
	}

}
