package edu.umich.verdict.relation.condition;

public abstract class CondVisitor<T> {
	
	public CondVisitor() {};
	
	public T visit(Cond cond) {
		return cond.accept(this);
	}
	
	public abstract T call(Cond cond);

}
