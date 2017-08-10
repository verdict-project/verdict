package edu.umich.verdict.relation.condition;

public class NullCond extends Cond {

	@Override
	public String toString() {
		return "NULL";
	}

	@Override
	public Cond withTableSubstituted(String newTab) {
		return this;
	}
	
	@Override
	public String toSql() {
		return toString();
	}

    @Override
    public boolean equals(Cond o) {
        if (o instanceof NullCond) {
            return true;
        }
        return false;
    }

}
