package edu.umich.verdict.relation.expr;

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
		this.col = col.toLowerCase();
		this.tab = (tab != null)? tab.toLowerCase() : tab;
		this.schema = (schema != null)? schema.toLowerCase() : schema;
	}
	
	public static ColNameExpr from(String expr) {
		String[] t = expr.split("\\.");
		if (t.length > 2) {
			return new ColNameExpr(t[2], t[1], t[0]);
		} else if (t.length == 2) {
			return new ColNameExpr(t[1], t[0]);
		} else {
			return new ColNameExpr(t[0]);
		}
	}
	
	public String getCol() {
		return col;
	}
	
	public String getTab() {
		return tab;
	}
	
	public String getSchema() {
		return schema;
	}
	
	public void setTab(String tab) {
		this.tab = tab;
	}

	@Override
	public String toString() {
		if (tab == null) {
			return String.format("%s", col);
		} else {
			return String.format("%s.%s", tab, col);
		}
//		
//		if (schema == null) {
//			if (tab == null) {
//				return String.format("%s", col);
//			} else {
//				return String.format("%s.%s", tab, col);
//			}
//		} else {
//			return String.format("%s.%s.%s", schema, tab, col);
//		}
	}
	
	@Override
	public <T> T accept(ExprVisitor<T> v) {
		return v.call(this);
	}

}
