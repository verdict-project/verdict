package edu.umich.verdict.relation.condition;

public class TrueFalseCond extends Cond {
    
    private boolean isTrue = true;
    
    public TrueFalseCond(boolean isTrue) {
        this.isTrue = isTrue;
    }

    public boolean isTrue() {
        return isTrue;
    }

    public void setTrue(boolean isTrue) {
        this.isTrue = isTrue;
    }

    @Override
    public Cond withTableSubstituted(String newTab) {
        return this;
    }

    @Override
    public Cond withNewTablePrefix(String newPrefix) {
        return this;
    }

    @Override
    public String toString() {
        return (isTrue)? "TRUE" : "FALSE";
    }

    @Override
    public String toSql() {
        return toString();
    }

    @Override
    public boolean equals(Cond o) {
        if (o instanceof TrueFalseCond) {
            if (isTrue() == ((TrueFalseCond) o).isTrue()) {
                return true;
            }
        }
        return false;
    }

}
