package edu.umich.verdict.relation.expr;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.parser.VerdictSQLParser.ExpressionContext;

public class ConstantExpr extends Expr {
	
	private Object value;

	public ConstantExpr(VerdictContext vc, Object value) {
	    super(vc);
		this.value = value;
	}
	
	public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public static ConstantExpr from(VerdictContext vc, Object value) {
		return new ConstantExpr(vc, value);
	}
	
	public static ConstantExpr from(VerdictContext vc, String value) {
		return from(vc, (Object) value); 
	}
	
	public static ConstantExpr from(VerdictContext vc, ExpressionContext ctx) {
		return from(vc, ctx.getText());
	}

	@Override
	public String toString() {
		return value.toString();
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
    public int hashCode() {
	    return value.hashCode();
	}

    @Override
    public boolean equals(Expr o) {
        if (o instanceof ConstantExpr) {
            return getValue().toString().equals(((ConstantExpr) o).getValue().toString());
        }
        return false;
    }
}
