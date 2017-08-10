package edu.umich.verdict.relation.condition;

import edu.umich.verdict.relation.expr.Expr;

public class IsCond extends Cond {

	private Expr left;
	
	private Cond right;
	
	public Expr getLeft() {
        return left;
    }

    public void setLeft(Expr left) {
        this.left = left;
    }

    public Cond getRight() {
        return right;
    }

    public void setRight(Cond right) {
        this.right = right;
    }

    public IsCond(Expr left, Cond right) {
		this.left = left;
		this.right = right;
	}
	
	@Override
	public String toString() {
		return String.format("(%s IS %s)", left, right);
	}
	
	@Override
	public Cond withTableSubstituted(String newTab) {
		return new IsCond(left.withTableSubstituted(newTab), right.withTableSubstituted(newTab));
	}
	
	@Override
	public String toSql() {
		return String.format("(%s IS %s)", left.toSql(), right.toSql());
	}

    @Override
    public boolean equals(Cond o) {
        if (o instanceof IsCond) {
            return getLeft().equals(((IsCond) o).getLeft()) && getRight().equals(((IsCond) o).getRight());
        }
        return false;
    }

}
