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

import java.util.ArrayList;
import java.util.List;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.parser.VerdictSQLParser.Case_exprContext;
import edu.umich.verdict.parser.VerdictSQLParser.ExpressionContext;
import edu.umich.verdict.parser.VerdictSQLParser.Search_conditionContext;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

/**
 * For (CASE (WHEN condition THEN expression)+ ELSE expression END)
 * 
 * @author Yongjoo Park
 *
 */
public class CaseExpr extends Expr {

    private List<Cond> conditions;

    private List<Expr> expressions;

    public CaseExpr(VerdictContext vc, List<Cond> conditions, List<Expr> expressions) {
        super(vc);
        if (conditions.size() != expressions.size() && conditions.size() + 1 != expressions.size()) {
            VerdictLogger.warn(this, String.format(
                    "Incorrect number of conditions (%d) for the number of expressions (%d) in a case expression.",
                    conditions.size(), expressions.size()));
        }

        this.conditions = conditions;
        this.expressions = expressions;
    }

    public List<Cond> getConditions() {
        return conditions;
    }

    public List<Expr> getExpressions() {
        return expressions;
    }

    public static CaseExpr from(VerdictContext vc, String expr) {
        VerdictSQLParser p = StringManipulations.parserOf(expr);
        return from(vc, p.case_expr());
    }

    public static CaseExpr from(VerdictContext vc, Case_exprContext ctx) {
        List<Cond> conds = new ArrayList<Cond>();
        for (Search_conditionContext c : ctx.search_condition()) {
            conds.add(Cond.from(vc, c));
        }
        List<Expr> exprs = new ArrayList<Expr>();
        for (ExpressionContext e : ctx.expression()) {
            exprs.add(Expr.from(vc, e));
        }
        return new CaseExpr(vc, conds, exprs);
    }

    @Override
    public String toString() {
        StringBuilder sql = new StringBuilder(100);
        sql.append("(CASE");
        for (int i = 0; i < conditions.size(); i++) {
            sql.append(String.format(" WHEN %s THEN %s", conditions.get(i), expressions.get(i)));
        }
        if (expressions.size() > conditions.size()) {
            sql.append(String.format(" ELSE %s", expressions.get(expressions.size() - 1)));
        }
        sql.append(" END)");
        return sql.toString();
    }

    @Override
    public <T> T accept(ExprVisitor<T> v) {
        return v.call(this);
    }

    @Override
    public Expr withTableSubstituted(String newTab) {
        List<Cond> newConds = new ArrayList<Cond>();
        for (Cond c : conditions) {
            newConds.add(c.withTableSubstituted(newTab));
        }

        List<Expr> newExprs = new ArrayList<Expr>();
        for (Expr e : expressions) {
            newExprs.add(e.withTableSubstituted(newTab));
        }

        return new CaseExpr(vc, newConds, newExprs);
    }

    @Override
    public Expr withNewTablePrefix(String newPrefix) {
        List<Cond> newConds = new ArrayList<Cond>();
        for (Cond c : conditions) {
            newConds.add(c.withNewTablePrefix(newPrefix));
        }

        List<Expr> newExprs = new ArrayList<Expr>();
        for (Expr e : expressions) {
            newExprs.add(e.withNewTablePrefix(newPrefix));
        }

        return new CaseExpr(vc, newConds, newExprs);
    }

    @Override
    public String toSql() {
        StringBuilder sql = new StringBuilder(100);
        sql.append("(CASE");
        for (int i = 0; i < conditions.size(); i++) {
            sql.append(String.format(" WHEN %s THEN %s", conditions.get(i).toSql(), expressions.get(i).toSql()));
        }
        if (expressions.size() > conditions.size()) {
            sql.append(String.format(" ELSE %s", expressions.get(expressions.size() - 1).toSql()));
        }
        sql.append(" END)");
        return sql.toString();
    }

    @Override
    public boolean equals(Expr o) {
        if (o instanceof CaseExpr) {
            return getConditions().equals(((CaseExpr) o).getConditions())
                    && getExpressions().equals(((CaseExpr) o).getExpressions());
        }
        return false;
    }

    @Override
    public int hashCode() {
        int s = 0;
        for (Cond c : getConditions()) {
            s += c.hashCode();
        }
        for (Expr e : getExpressions()) {
            s += e.hashCode();
        }
        return s;
    }

}
