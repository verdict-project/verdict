package edu.umich.verdict.relation.expr;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.util.VerdictLogger;

public class ColNameExpr extends Expr {

    private String col;

    private String tab;

    private String schema;

    public ColNameExpr(VerdictContext vc, String col) {
        this(vc, col, null, null);
    }

    public ColNameExpr(VerdictContext vc, String col, String tab) {
        this(vc, col, tab, null);
    }

    public ColNameExpr(VerdictContext vc, String col, String tab, String schema) {
        super(vc);
        
        this.col = col.toLowerCase().replace("\"", "").replace("`", "");
        this.tab = (tab != null)? tab.toLowerCase().replace("\"", "").replace("`", "") : tab;
        this.schema = (schema != null)? schema.toLowerCase().replace("\"", "").replace("`", "") : schema;
    }

    public static ColNameExpr from(VerdictContext vc, String expr) {
        String[] t = expr.split("\\.");
        if (t.length > 2) {
            return new ColNameExpr(vc, t[2], t[1], t[0]);
        } else if (t.length == 2) {
            return new ColNameExpr(vc, t[1], t[0]);
        } else {
            return new ColNameExpr(vc, t[0]);
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
        if (schema == null) {
            if (tab == null) {
                return String.format("%s", quote(col));
            }
            return String.format("%s.%s", tab, quote(col));
        }
        return String.format("%s.%s.%s", schema, tab, quote(col));
    }

    public String getText() {
        if (tab == null) {
            return String.format("%s", col);
        } else {
            return String.format("%s.%s", tab, col);
        }
    }

    @Override
    public <T> T accept(ExprVisitor<T> v) {
        return v.call(this);
    }

    @Override
    public Expr withTableSubstituted(String newTab) {
        return new ColNameExpr(vc, col, newTab);
    }

    @Override
    public String toSql() {
        return toString();
    }

    @Override
    public boolean equals(Expr o) {
        if (o instanceof ColNameExpr) {
            if (getCol().equals(((ColNameExpr) o).getCol())) {
                if (getTab() == null) {
                    return true;
                } else {
                    if (((ColNameExpr) o).getCol() != null
                        && getCol().equals(((ColNameExpr) o).getCol())) {
                        if (getSchema() == null) {
                            return true;
                        } else {
                            if (((ColNameExpr) o).getSchema() != null
                                && getSchema().equals(((ColNameExpr) o).getSchema())) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }
}
