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
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public abstract class Expr {

    protected VerdictContext vc;

    protected void setVerdictContext(VerdictContext vc) {
        this.vc = vc;
    }

    public Expr(VerdictContext vc) {
        if (vc == null) {
            VerdictLogger.error(this, "null VerdictContext is being set.");
        }
        this.vc = vc;
    }

    public VerdictContext getVerdictContext() {
        return vc;
    }

    private static Expr from(String expr) {
        return from(VerdictContext.dummyContext(), expr);
    }

    public static Expr from(VerdictContext vc, String expr) {
        VerdictSQLParser p = StringManipulations.parserOf(expr);
        return from(vc, p.expression());
    }

    private static Expr from(VerdictContext vc, Object obj) {
        return from(vc, obj.toString());
    }

    private static Expr from(VerdictSQLParser.ExpressionContext ctx) {
        return from(VerdictContext.dummyContext(), ctx);
    }

    public static Expr from(VerdictContext vc, VerdictSQLParser.ExpressionContext ctx) {
        if (vc == null)
            vc = VerdictContext.dummyContext();
        ExpressionGen g = new ExpressionGen(vc);
        return g.visit(ctx);
    }

    // public abstract String toString(VerdictContext vc);

    // to Sql String; use getText() to get the pure string representation.
    @Override
    public String toString() {
        return "Expr Base";
    }

    public String toStringWithoutQuote() {
        return StringManipulations.stripQuote(toString());
    }

    public static String quote(VerdictContext vc, String s) {
        if (vc == null) {
            VerdictLogger.error("null VerdictContext");
            return String.format("`%s`", s);
        } else {
            String q = vc.getDbms().getQuoteString();
            return String.format("%s%s%s", q, s, q);
        }
    }

    public String quote(String s) {
        return Expr.quote(vc, s);

    }

    public String getText() {
        return toString();
    }

    public Expr accept(ExprModifier v) {
        return v.call(this);
    }

    public abstract <T> T accept(ExprVisitor<T> v);

    public boolean isagg() {
        return false;
    }

    public boolean isCountDistinct() {
        return false;
    }

    public boolean isCount() {
        return false;
    }

    public boolean isMax() {
        return false;
    }

    public boolean isMin() {
        return false;
    }

    public boolean isMeanLikeAggregate() {
        return false;
    }

    public List<String> extractColNames() {
        ExprVisitor<List<String>> v = new ExprVisitor<List<String>>() {
            private List<String> cols = new ArrayList<String>();

            @Override
            public List<String> call(Expr expr) {
                if (expr instanceof ColNameExpr) {
                    cols.add(((ColNameExpr) expr).getCol());
                } else if (expr instanceof FuncExpr) {
                    for (Expr e : ((FuncExpr) expr).getExpressions()) {
                        visit(e);
                    }
                }
                return cols;
            }
        };
        List<String> cols = v.visit(this);
        return cols;
    }

    public abstract Expr withTableSubstituted(String newTab);

    public abstract Expr withNewTablePrefix(String newPrefix);

    public abstract String toSql();

    @Override
    public abstract int hashCode();

    @Override
    public boolean equals(Object o) {
        if (o instanceof Expr) {
            return equals((Expr) o);
        } else {
            return false;
        }
    }

    public abstract boolean equals(Expr o);

    public List<FuncExpr> extractFuncExpr() {
        ExprVisitor<List<FuncExpr>> collectAggFuncs = new ExprVisitor<List<FuncExpr>>() {
            private List<FuncExpr> seen = new ArrayList<FuncExpr>();

            public List<FuncExpr> call(Expr expr) {
                if (expr instanceof FuncExpr) {
                    seen.add((FuncExpr) expr);
                }
                return seen;
            }
        };
        List<FuncExpr> funcs = collectAggFuncs.visit(this);
        return funcs;
    }

}

class ExpressionGen extends VerdictSQLBaseVisitor<Expr> {

    private VerdictContext vc;

    public ExpressionGen(VerdictContext vc) {
        this.vc = vc;
    }

    @Override
    public Expr visitInterval(VerdictSQLParser.IntervalContext ctx) {
        IntervalExpr.Unit unit = IntervalExpr.Unit.DAY;
        if (ctx.DAY() != null || ctx.DAYS() != null) {
            unit = IntervalExpr.Unit.DAY;
        } else if (ctx.MONTH() != null || ctx.MONTHS() != null) {
            unit = IntervalExpr.Unit.MONTH;
        } else if (ctx.YEAR() != null || ctx.YEARS() != null) {
            unit = IntervalExpr.Unit.YEAR;
        }
        return new IntervalExpr(vc, ctx.constant_expression().getText(), unit);
    }

    @Override
    public Expr visitPrimitive_expression(VerdictSQLParser.Primitive_expressionContext ctx) {
        return ConstantExpr.from(vc, ctx.getText());
    }

    @Override
    public Expr visitColumn_ref_expression(VerdictSQLParser.Column_ref_expressionContext ctx) {
        return ColNameExpr.from(vc, ctx.getText());
    }

    @Override
    public Expr visitBinary_operator_expression(VerdictSQLParser.Binary_operator_expressionContext ctx) {
        return new BinaryOpExpr(vc, visit(ctx.expression(0)), visit(ctx.expression(1)), ctx.op.getText());
    }

    @Override
    public Expr visitFunction_call_expression(VerdictSQLParser.Function_call_expressionContext ctx) {
        return FuncExpr.from(vc, ctx.function_call());
    }

    @Override
    public Expr visitCase_expr(VerdictSQLParser.Case_exprContext ctx) {
        return CaseExpr.from(vc, ctx);
    }

    @Override
    public Expr visitBracket_expression(VerdictSQLParser.Bracket_expressionContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public Expr visitSubquery_expression(VerdictSQLParser.Subquery_expressionContext ctx) {
        return SubqueryExpr.from(vc, ctx);
    }

}
