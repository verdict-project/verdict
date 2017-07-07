package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.relation.expr.SelectElem;

/**
 * Stores information about what samples to use to compute multiple expressions. A single SampleGroup instance stores
 * the mapping from a set of samples to a list of expressions to answer using the set, and this class includes multiple number
 * of such SampleGroup instances so that they can answer the user-submitted query when combined.
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
	
	@Override
	public String toString() {
		return sampleGroups.toString();
	}
	
	public String toPrettyString() {
		StringBuilder s = new StringBuilder();
		for (SampleGroup g : sampleGroups) {
			s.append(g.toString()); s.append("\n");
		}
		return s.toString();
	}
	
	public double cost() {
		if (cachedCost != null) return cachedCost;
		
		double cost = 0;
		for (SampleGroup g : sampleGroups) {
			cost += g.cost();
		}
		cachedCost = cost;
		return cost;
	}
	
	/**
	 * Computes the harmonic mean of the sampling probabilities of the SampleGroup instances.
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
	 *  creates and returns a new SamplePlan by merging the new sample group.
	 * @param group
	 * @return
	 */
	public SamplePlan createByMerge(SampleGroup group) {
		SamplePlan copy = this.duplicate();
		
		// check if there's an existing sample group into which a new sample group can be combined.
		boolean merged = false;
		for (SampleGroup g : copy.sampleGroups) {
			if (g.isEqualSample(group)) {
				g.addElem(group.getElems());
				merged = true;
				break;
			}
		}
		
		if (!merged) {
			copy.sampleGroups.add(group);
		}
		
		return copy;
	}
	
	public List<Pair<Set<SampleParam>, List<SelectElem>>> unroll() {
		List<Pair<Set<SampleParam>, List<SelectElem>>> unrolled = new ArrayList<Pair<Set<SampleParam>, List<SelectElem>>>();
		for (SampleGroup g : sampleGroups) {
			unrolled.add(g.unroll());
		}
		return unrolled;
	}
}
