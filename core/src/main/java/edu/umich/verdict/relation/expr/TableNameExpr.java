package edu.umich.verdict.relation.expr;

import edu.umich.verdict.VerdictContext;

// Currently not used.
public class TableNameExpr extends Expr {

	public TableNameExpr(VerdictContext vc) {
        super(vc);
    }

    @Override
	public <T> T accept(ExprVisitor<T> v) {
		return v.call(this);
	}

	@Override
	public Expr withTableSubstituted(String newTab) {
		return this;
	}
	
	@Override
	public String toSql() {
		return toString();
	}

    @Override
    public boolean equals(Expr o) {
        if (o instanceof TableNameExpr) {
            return true;
        }
        return false;
    }

}
