package edu.umich.verdict.relation.expr;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class ColNameExpr extends Expr {
	
	private String col;
	
	private String tab;
	
	private String schema;
	
	public ColNameExpr(String col) {
		this(col, null, null);
	}
	
	public ColNameExpr(String col, String tab) {
		this(col, tab, null);
	}

	public ColNameExpr(String col, String tab, String schema) {
		this.col = col;
		this.tab = tab;
		this.schema = schema;
	}

	@Override
	public String toString(VerdictContext vc) {
		if (schema == null) {
			if (tab == null) {
				return String.format("%s.%s", vc.getCurrentSchema(), col);
			} else {
				return String.format("%s.%s", tab, col);
			}
		} else {
			return String.format("%s.%s.%s", schema, tab, col);
		}
	}
	
	public Expr accept(ExprVisitor v) throws VerdictException {
		return v.call(this);
	}

}
