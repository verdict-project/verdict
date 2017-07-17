package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.SelectElem;

/**
 * Indicates that {@link SampleGroup#elems} can be computed by joining {@link SampleGroup#sample}.
 * @author Yongjoo Park
 *
 */
public class SampleGroup {
	
	private ApproxRelation sample;
	
	private List<Expr> elems;
	
//	private double cost;
	
	/**
	 * 
	 * @param sample ApproxRelation instance
	 * @param elems Expressions that can be answered using the sample.
	 */
	public SampleGroup(ApproxRelation sample, List<Expr> elems) {
		this.sample = sample;
		
		this.elems = new ArrayList<Expr>();
		this.elems.addAll(elems);
	}
	
	public ApproxRelation getSample() {
		return sample;
	}
	
	public void setSample(ApproxRelation a) {
		sample = a;
	}
	
	public double samplingProb() {
		return sample.samplingProbability();
	}
	
	public double cost() {
		return sample.cost();
	}
	
	public String sampleType() {
		return sample.sampleType();
//		String type = null;
//		for (ApproxRelation param : samples) {
//			if (type == null) {
//				type = param.sampleType;
//			} else {
//				if (type.equals("uniform")) {
//					if (param.sampleType.equals("stratified")) {
//						type = "stratified";
//					} else {
//						type = "uniform";
//					}
//				} else if (type.equals("stratified")) {
//					type = "stratified";
//				} else if (type.equals("universe")) {
//					type = "universe";
//				}
//			}
//		}
//		return type;
	}
	
	public List<Expr> getElems() {
		return elems;
	}
	
//	public Set<SampleParam> sampleSet() {
//		return samples;
//	}
	
	@Override
	public String toString() {
		return elems.toString() + " =>\n" + sample.toString(); 
	}
	
	public boolean isEqualSample(SampleGroup o) {
		ApproxRelation source1 = ((ApproxAggregatedRelation) sample).getSource();
		ApproxRelation source2 = ((ApproxAggregatedRelation) o.getSample()).getSource();
		return source1.equals(source2);
	}
	
	public void addElem(List<Expr> e) {
		elems.addAll(e);
	}
	
//	public Pair<Set<SampleParam>, List<Expr>> unroll() {
//		return Pair.of(samples, elems);
//	}
	
	public SampleGroup duplicate() {
//		Set<SampleParam> copiedSamples = new HashSet<SampleParam>(samples);
		List<Expr> copiedElems = new ArrayList<Expr>(elems);
		return new SampleGroup(sample, copiedElems);
	}
}
