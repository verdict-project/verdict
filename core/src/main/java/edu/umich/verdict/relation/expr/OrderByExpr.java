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

public class OrderByExpr extends Expr {

    private Expr expr;

    private Optional<String> direction;

    public OrderByExpr(VerdictContext vc, Expr expr, String direction) {
        super(vc);
        this.expr = expr;
        this.direction = Optional.fromNullable(direction);
    }

    public OrderByExpr(VerdictContext vc, Expr expr) {
        this(vc, expr, null);
    }

    public Expr getExpression() {
        return expr;
    }

    public Optional<String> getDirection() {
        return direction;
    }

    public static OrderByExpr from(final VerdictContext vc, String expr) {
        VerdictSQLParser p = StringManipulations.parserOf(expr);
        VerdictSQLBaseVisitor<OrderByExpr> v = new VerdictSQLBaseVisitor<OrderByExpr>() {
            @Override
            public OrderByExpr visitOrder_by_expression(VerdictSQLParser.Order_by_expressionContext ctx) {
                String dir = (ctx.ASC() != null) ? "ASC" : ((ctx.DESC() != null) ? "DESC" : null);
                return new OrderByExpr(vc, Expr.from(vc, ctx.expression()), dir);
            }
        };
        return v.visit(p.order_by_expression());
    }

    @Override
    public String toString() {
        if (direction.isPresent()) {
            return expr.toString() + " " + direction.get();
        } else {
            return expr.toString();
        }
    }

    @Override
    public <T> T accept(ExprVisitor<T> v) {
        return v.call(this);
    }

    @Override
    public OrderByExpr withTableSubstituted(String newTab) {
        Expr newExpr = expr.withTableSubstituted(newTab);
        return new OrderByExpr(vc, newExpr, direction.orNull());
    }

    @Override
    public Expr withNewTablePrefix(String newPrefix) {
        Expr newExpr = expr.withNewTablePrefix(newPrefix);
        return new OrderByExpr(vc, newExpr, direction.orNull());
    }

    @Override
    public String toSql() {
        return toString();
    }

    @Override
    public int hashCode() {
        return expr.hashCode();
    }

    @Override
    public boolean equals(Expr o) {
        if (o instanceof OrderByExpr) {
            return getExpression().equals(((OrderByExpr) o).getExpression())
                    && getDirection().equals(((OrderByExpr) o).getDirection());
        }
        return false;
    }

}
