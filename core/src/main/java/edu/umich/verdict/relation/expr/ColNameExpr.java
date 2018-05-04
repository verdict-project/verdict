/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.relation.expr;

import edu.umich.verdict.VerdictContext;

public class ColNameExpr extends Expr implements Comparable {

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
        this.tab = (tab != null) ? tab.toLowerCase().replace("\"", "").replace("`", "") : tab;
        this.schema = (schema != null) ? schema.toLowerCase().replace("\"", "").replace("`", "") : schema;
    }

    // This is basically a copy constructor.
    public static ColNameExpr from(VerdictContext vc, ColNameExpr expr) {
        return new ColNameExpr(vc, expr.getCol(), expr.getTab(), expr.getSchema());
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

    public void setSchema(String schema) {
        this.schema = schema;
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

    public String toStringWithoutQuote() {
        if (schema == null) {
            if (tab == null) {
                return String.format("%s", col);
            }
            return String.format("%s.%s", tab, col);
        }
        return String.format("%s.%s.%s", schema, tab, col);
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
    public Expr withNewTablePrefix(String newPrefix) {
        return new ColNameExpr(vc, col, newPrefix + tab);
    }

    @Override
    public String toSql() {
        return toString();
    }

    @Override
    public int hashCode() {
        int s = col.hashCode();
        if (tab != null) {
            s += tab.hashCode();
        }
        if (schema != null) {
            s += schema.hashCode();
        }
        return s;
    }

    @Override
    public boolean equals(Expr o) {
        if (o instanceof ColNameExpr) {
            if (getCol().equals(((ColNameExpr) o).getCol())) {
                if (getTab() == null) {
                    return true;
                } else {
                    if (((ColNameExpr) o).getCol() != null && getCol().equals(((ColNameExpr) o).getCol())) {
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

    @Override
    public int compareTo(Object o) {
        ColNameExpr other = (ColNameExpr) o;
        return this.toString().compareTo(other.toString());
    }
}
