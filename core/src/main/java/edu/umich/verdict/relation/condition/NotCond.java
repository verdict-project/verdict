package edu.umich.verdict.relation.condition;

public class NotCond extends Cond {
	
	private Cond cond;
	
	public NotCond(Cond cond) {
		this.cond = cond;
	}
	
	public static NotCond from(Cond cond) {
		return new NotCond(cond);
	}

	@Override
	public String toString() {
		return String.format("NOT %s", cond);
	}
	
	@Override
	public Cond withTableSubstituted(String newTab) {
		return new NotCond(cond.withTableSubstituted(newTab));
	}
	
	@Override
	public String toSql() {
		return String.format("NOT %s", cond.toSql());
	}

    @Override
    public boolean equals(Cond o) {
        if (o instanceof NotCond) {
            return cond.equals(o);
        }
        return false;
    }
}
