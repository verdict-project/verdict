package edu.umich.verdict.relation.condition;


public abstract class CondModifier {

    public Cond visit(Cond cond) {
        return cond.accept(this);
    }

    public abstract Cond call(Cond cond);

}