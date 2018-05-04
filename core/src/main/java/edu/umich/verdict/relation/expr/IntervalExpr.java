package edu.umich.verdict.relation.expr;

import edu.umich.verdict.VerdictContext;

/**
 * Created by Dong Young Yoon on 4/3/18.
 */
public class IntervalExpr extends Expr{

    private Object value;
    private Unit unit;

    public enum Unit {DAY, MONTH, YEAR}

    public IntervalExpr(VerdictContext vc, Object value, Unit unit) {
        super(vc);
        this.value = value;
        this.unit = unit;
    }

    public Object getValue() {
        return value;
    }

    public Unit getUnit() {
        return unit;
    }

    @Override
    public String toString() {
        String unitStr;
        switch (unit) {
            case DAY:
                unitStr = "days";
                break;
            case MONTH:
                unitStr = "months";
                break;
            case YEAR:
                unitStr = "years";
                break;
            default:
                unitStr = "days";
                break;
        }
        return String.format("interval %s %s", value.toString(), unitStr);
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
    public Expr withNewTablePrefix(String newPrefix) {
        return this;
    }

    @Override
    public String toSql() {
        return this.toString();
    }

    @Override
    public int hashCode() {
        return value.hashCode() + unit.hashCode();
    }

    @Override
    public boolean equals(Expr o) {
        if (o instanceof IntervalExpr) {
            IntervalExpr other = (IntervalExpr) o;
            if (this.value.toString().equals(other.value.toString()) && this.unit == other.unit) {
                return true;
            }
        }
        return false;
    }
}
