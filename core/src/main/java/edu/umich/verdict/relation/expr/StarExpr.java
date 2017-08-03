package edu.umich.verdict.relation.expr;

import edu.umich.verdict.VerdictContext;

public class StarExpr extends Expr {

    public StarExpr() {
        super(VerdictContext.dummyContext());
    }

    @Override
    public String toString() {
        return "*";
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

}
