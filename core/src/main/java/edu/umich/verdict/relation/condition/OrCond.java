package edu.umich.verdict.relation.condition;

import edu.umich.verdict.VerdictContext;

public class OrCond extends Cond {
	
	private Cond left;
	
	private Cond right;

	public OrCond(Cond left, Cond right) {
		this.left = left;
		this.right = right;
	}
	
	public static OrCond from(Cond left, Cond right) {
		return new OrCond(left, right);
	}

	@Override
	public String toString(VerdictContext vc) {
		return String.format("(%s) OR (%s)", left.toString(vc), right.toString(vc));
	}

}
