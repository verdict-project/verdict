package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.HashSet;
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
	
	public String sampleType() {
		String type = null;
		for (SampleParam param : samples) {
			if (type == null) {
				type = param.sampleType;
			} else {
				if (type.equals("uniform")) {
					if (param.sampleType.equals("stratified")) {
						type = "stratified";
					} else {
						type = "uniform";
					}
				} else if (type.equals("stratified")) {
					type = "stratified";
				} else if (type.equals("universe")) {
					type = "universe";
				}
			}
		}
		return type;
	}
	
	public List<SelectElem> getElems() {
		return elems;
	}
	
	public Set<SampleParam> sampleSet() {
		return samples;
	}
	
	@Override
	public String toString() {
		return samples.toString() + "=>" + elems.toString();
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
	
	public SampleGroup duplicate() {
		Set<SampleParam> copiedSamples = new HashSet<SampleParam>(samples);
		List<SelectElem> copiedElems = new ArrayList<SelectElem>(elems);
		return new SampleGroup(copiedSamples, copiedElems, samplingProb, cost);
	}
}
