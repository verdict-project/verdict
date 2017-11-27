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

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.util.StringManipulations;

public class SelectElem {

    private Expr expr;

    private Optional<String> alias;

    private VerdictContext vc;

    public VerdictContext getVerdictContext() {
        return vc;
    }

    public SelectElem(VerdictContext vc, Expr expr, String alias) {
        this.expr = expr;
        this.vc = vc;
        if (alias == null && !(expr instanceof StarExpr)) {
            // by default, we alias every expression except for *.
            this.alias = Optional.of(genColumnAlias(expr));
        } else {
            setAlias(alias);
        }
    }

    public SelectElem(VerdictContext vc, Expr expr) {
        this(vc, expr, null);
    }

    public static SelectElem from(VerdictContext vc, String elem) {
        VerdictSQLParser p = StringManipulations.parserOf(elem);
        return from(vc, p.select_list_elem());
    }

    public static SelectElem from(final VerdictContext vc, VerdictSQLParser.Select_list_elemContext ctx) {
        VerdictSQLBaseVisitor<SelectElem> v = new VerdictSQLBaseVisitor<SelectElem>() {
            @Override
            public SelectElem visitSelect_list_elem(VerdictSQLParser.Select_list_elemContext ctx) {
                SelectElem elem = null;
                if (ctx.STAR() != null) {
                    if (ctx.table_name() == null) {
                        elem = new SelectElem(vc, new StarExpr());
                    } else {
                        elem = new SelectElem(vc, new StarExpr(TableNameExpr.from(vc, ctx.table_name())));
                    }
                } else {
                    elem = new SelectElem(vc, Expr.from(vc, ctx.expression()));
                }

                if (ctx.column_alias() != null) {
                    elem.setAlias(ctx.column_alias().getText());
                }
                return elem;
            }
        };

        return v.visit(ctx);
    }

    private static int column_alias_num = 1;

    public static String genColumnAlias(Expr expr) {
        String a;
        if (expr instanceof ColNameExpr) {
            a = ((ColNameExpr) expr).getCol();
        } else {
            a = String.format("%s_%d", expr.getText().substring(0, 1), column_alias_num);
        }
        column_alias_num++;
        return a;
    }

    public Expr getExpr() {
        return expr;
    }

    public boolean aliasPresent() {
        return alias.isPresent();
    }

    public String getAlias() {
        if (alias.isPresent()) {
            return alias.get();
        } else {
            return null;
        }
    }

    public void setAlias(String alias) {
        if (alias == null) {
            this.alias = Optional.fromNullable(alias);
        } else {
            this.alias = Optional.of(alias.replace("\"", "").replace("`", "").toLowerCase());
        }
    }

    public boolean isagg() {
        return expr.isagg();
    }

    @Override
    public String toString() {
        if (alias.isPresent()) {
            return String.format("%s AS %s", expr.toString(), Expr.quote(vc, alias.get()));
        } else {
            return expr.toString();
        }
    }

}
