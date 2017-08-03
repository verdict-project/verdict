package edu.umich.verdict.relation.expr;

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.util.StringManipulations;

public class SelectElem {

    private Expr expr;

    private Optional<String> alias;

    public SelectElem(Expr expr, String alias) {
        this.expr = expr;
        if (alias == null) {
            if (expr.isagg()) {
                this.alias = Optional.of(genColumnAlias());		// aggregate expressions must be aliased.
            } else {
                this.alias = Optional.fromNullable(alias);
            }
        } else {
            setAlias(alias);
        }
    }

    public SelectElem(Expr expr) {
        this(expr, null);
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
                if (ctx.getText().equals("*")) {
                    elem = new SelectElem(new StarExpr());
                } else {
                    elem = new SelectElem(Expr.from(vc, ctx.expression()));
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

    public static String genColumnAlias() {
        String a = String.format("expr%d", column_alias_num);
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
        this.alias = Optional.of(alias.replace("\"", "").replace("`", ""));
    }

    public boolean isagg() {
        return expr.isagg();
    }

    @Override
    public String toString() {
        if (alias.isPresent()) {
            return String.format("%s AS %s", expr.toString(), expr.quote(alias.get()));
        } else {
            return expr.toString();
        }
    }

}
