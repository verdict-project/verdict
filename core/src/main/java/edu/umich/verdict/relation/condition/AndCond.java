package edu.umich.verdict.relation.condition;

import edu.umich.verdict.VerdictContext;

public class AndCond extends Cond {
	
	private Cond left;
	
	private Cond right;

	public AndCond(Cond left, Cond right) {
		this.left = left;
		this.right = right;
	}
	
	public static AndCond from(Cond left, Cond right) {
		return new AndCond(left, right);
	}
	
	public Cond getLeft() {
		return left;
	}
	
	public Cond getRight() {
		return right;
	}
	
	@Override
	public Cond accept(CondModifier v) {
		return v.call(this);
	}

	@Override
	public String toString(VerdictContext vc) {
		return String.format("(%s) AND (%s)", left.toString(vc), right.toString(vc));
	}

}
