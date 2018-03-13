package edu.umich.verdict.relation.condition;


import java.util.ArrayList;
import java.util.List;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.parser.VerdictSQLParser.ExpressionContext;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.SubqueryExpr;
import edu.umich.verdict.util.VerdictLogger;


public class ExistsCond extends Cond {

    Expr subquery;



    public ExistsCond( Expr subquery) {
        this.subquery = subquery;
    }
    public Expr getSubquery() {
        return subquery;
    }

    public static ExistsCond from(VerdictContext vc, VerdictSQLParser.Exists_predicateContext ctx) {
        if(ctx.subquery() == null){
            VerdictLogger.error("Exists should folow by a subquery");
        }

        Expr subquery = SubqueryExpr.from(vc,ctx.subquery());
        return new ExistsCond(subquery);

    }


    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(" EXISTS (");
        sql.append(subquery.toSql());
        sql.append(")");
        return sql.toString();
    }


    @Override
    public boolean equals(Cond o) {
        if (o instanceof Cond) {

            return subquery.equals(((ExistsCond) o).subquery);
        }
        return false;
    }

    @Override
    public Cond withTableSubstituted(String newTab) {
        return this;
    }

}
