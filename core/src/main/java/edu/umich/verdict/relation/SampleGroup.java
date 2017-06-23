package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.relation.expr.SelectElem;

public class SampleGroup {
	
	private Set<SampleParam> samples;
	
	private List<SelectElem> elems;
	
	private double samplingProb;
	
	private double cost;
	
	public SampleGroup(Set<SampleParam> samples, List<SelectElem> elems, double samplingProb, double cost) {
		this.samples = samples;
		this.samplingProb = samplingProb;
		this.cost = cost;
		
		this.elems = new ArrayList<SelectElem>();
		this.elems.addAll(elems);
	}
	
	public double samplingProb() {
		return samplingProb;
	}
	
	public double cost() {
		return cost;
	}
	
	public List<SelectElem> getElems() {
		return elems;
	}
	
	public Set<SampleParam> sampleSet() {
		return samples;
	}
	
	public boolean isEqualSample(SampleGroup o) {
		return this.samples.equals(o.samples);
	}
	
	public void addElem(List<SelectElem> e) {
		elems.addAll(e);
	}
	
	public Pair<Set<SampleParam>, List<SelectElem>> unroll() {
		return Pair.of(samples, elems);
	}
}
