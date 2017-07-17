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

	public Cond getLeft() {
		return left;
	}

	public void setLeft(Cond left) {
		this.left = left;
	}

	public Cond getRight() {
		return right;
	}

	public void setRight(Cond right) {
		this.right = right;
	}

	@Override
	public Cond accept(CondModifier v) {
		return from(left.accept(v), right.accept(v));
	}

	@Override
	public String toString() {
		return String.format("(%s) OR (%s)", left.toString(), right.toString());
	}

	@Override
	public Cond withTableSubstituted(String newTab) {
		return new OrCond(left.withTableSubstituted(newTab), right.withTableSubstituted(newTab));
	}
}
