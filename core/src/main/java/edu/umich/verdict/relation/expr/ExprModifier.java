/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;

public abstract class ExprModifier {

    protected VerdictContext vc;

    public ExprModifier(VerdictContext vc) {
        this.vc = vc;
    }

    public Expr visit(Expr expr) {
        return expr.accept(this);
    }

    public Expr call(Expr expr) {
        return visitExpr(expr);
    }

    public Expr visitExpr(Expr expr) {
        if (expr instanceof BinaryOpExpr) {
            return visitBinaryOpExpr((BinaryOpExpr) expr);
        } else if (expr instanceof CaseExpr) {
            return visitCaseExpr((CaseExpr) expr);
        } else if (expr instanceof ColNameExpr) {
            return visitColNameExpr((ColNameExpr) expr);
        } else if (expr instanceof ConstantExpr) {
            return visitConstantExpr((ConstantExpr) expr);
        } else if (expr instanceof FuncExpr) {
            return visitFuncExpr((FuncExpr) expr);
        } else if (expr instanceof OrderByExpr) {
            return visitOrderByExpr((OrderByExpr) expr);
        } else if (expr instanceof SubqueryExpr) {
            return visitSubqueryExpr((SubqueryExpr) expr);
        }

        return expr;
    }

    public BinaryOpExpr visitBinaryOpExpr(BinaryOpExpr expr) {
        Expr left = visitExpr(expr.getLeft());
        Expr right = visitExpr(expr.getRight());
        return new BinaryOpExpr(vc, left, right, expr.getOp());
    }

    public CaseExpr visitCaseExpr(CaseExpr expr) {
        List<Expr> newExprs = new ArrayList<Expr>();
        List<Expr> exprs = expr.getExpressions();
        for (Expr e : exprs) {
            newExprs.add(visitExpr(e));
        }
        return new CaseExpr(vc, expr.getConditions(), newExprs);
    }

    public ColNameExpr visitColNameExpr(ColNameExpr expr) {
        return expr;
    }

    public ConstantExpr visitConstantExpr(ConstantExpr expr) {
        return expr;
    }

    public FuncExpr visitFuncExpr(FuncExpr expr) {
        List<Expr> newExprs = new ArrayList<Expr>();
        List<Expr> exprs = expr.getExpressions();
        for (Expr e : exprs) {
            newExprs.add(visitExpr(e));
        }
        return new FuncExpr(expr.getFuncName(), newExprs, expr.getOverClause());
    }

    public OrderByExpr visitOrderByExpr(OrderByExpr expr) {
        Expr newExpr = visitExpr(expr.getExpression());
        Optional<String> dir = expr.getDirection();

        if (dir.isPresent()) {
            return new OrderByExpr(vc, newExpr, dir.get());
        } else {
            return new OrderByExpr(vc, newExpr);
        }
    }

    public SubqueryExpr visitSubqueryExpr(SubqueryExpr expr) {
        return expr;
    }

    // public Relation call(Relation r) {
    // return r;
    // }

}
