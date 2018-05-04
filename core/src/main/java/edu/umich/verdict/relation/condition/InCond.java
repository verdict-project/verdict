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

package edu.umich.verdict.relation.condition;

import java.util.ArrayList;
import java.util.List;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.parser.VerdictSQLParser.ExpressionContext;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.SubqueryExpr;
import edu.umich.verdict.util.VerdictLogger;

public class InCond extends Cond {

    Expr left;

    boolean not;

    List<Expr> expressionList;
    
    Expr subquery;

    public InCond(Expr left, boolean not, List<Expr> expressionList) {
        this.left = left;
        this.not = not;
        this.expressionList = expressionList;
    }
    
    public InCond(Expr left, boolean not, Expr subquery) {
        this.left = left;
        this.not = not;
        this.subquery = subquery;
    }

    public Expr getLeft() {
        return left;
    }

    public void setLeft(Expr left) {
        this.left = left;
    }

    public boolean isNot() {
        return not;
    }

    public void setNot(boolean not) {
        this.not = not;
    }

    public List<Expr> getExpressionList() {
        return expressionList;
    }

    public void setExpressionList(List<Expr> expressionList) {
        this.expressionList = expressionList;
    }
    
    public Expr getSubquery() {
        return subquery;
    }
    

    public static InCond from(VerdictContext vc, VerdictSQLParser.In_predicateContext ctx) {
        if (ctx.subquery() != null) {
            VerdictLogger.error("Verdict currently does not support IN + subquery condition.");
            Expr left = Expr.from(vc, ctx.expression());
            boolean not = (ctx.NOT() != null) ? true : false;
            Expr subquery = SubqueryExpr.from(vc, ctx.subquery());
            return new InCond(left, not, subquery);
        } else {
            Expr left = Expr.from(vc, ctx.expression());
            boolean not = (ctx.NOT() != null) ? true : false;
            List<Expr> expressionList = new ArrayList<Expr>();

            for (ExpressionContext ectx : ctx.expression_list().expression()) {
                expressionList.add(Expr.from(vc, ectx));
            }

            return new InCond(left, not, expressionList);
        }
    }

    @Override
    public Cond withTableSubstituted(String newTab) {
        if (expressionList == null) {
            return this;
        }
        
        List<Expr> newExpressions = new ArrayList<Expr>();
        for (Expr expr : expressionList) {
            newExpressions.add(expr.withTableSubstituted(newTab));
        }
        return this;
    }

    @Override
    public Cond withNewTablePrefix(String newPrefix) {
        if (expressionList == null) {
            return this;
        }

        Expr newLeft = left.withNewTablePrefix(newPrefix);
        List<Expr> newExpressions = new ArrayList<Expr>();
        for (Expr expr : expressionList) {
            newExpressions.add(expr.withNewTablePrefix(newPrefix));
        }
        return new InCond(newLeft, not, newExpressions);
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(left.toSql());
        if (not)
            sql.append(" NOT");
        sql.append(" IN (");
        
        if (subquery != null) {
            sql.append(subquery.toSql());
        } else {
            for (int i = 0; i < expressionList.size(); i++) {
                if (i > 0)
                    sql.append(", ");
                sql.append(expressionList.get(i).toSql());
            }
        }

        sql.append(")");
        return sql.toString();
    }

    @Override
    public boolean equals(Cond o) {
        if (o instanceof InCond) {
            if (subquery != null && ((InCond) o).subquery != null) {
                return subquery.equals(((InCond) o).subquery);
            } else if (expressionList != null && ((InCond) o).expressionList != null) {
                return getLeft().equals(((InCond) o).getLeft()) && (isNot() == ((InCond) o).isNot())
                        && getExpressionList().equals(((InCond) o).getExpressionList());
            }
        }
        return false;
    }

}
