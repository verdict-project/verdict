package edu.umich.verdict.relation.condition;

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
	public String toString() {
		return String.format("(%s) OR (%s)", left.toString(), right.toString());
	}

}
