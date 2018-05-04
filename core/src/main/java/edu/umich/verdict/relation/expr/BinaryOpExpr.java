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

public class BinaryOpExpr extends Expr {

    private Expr left;

    private Expr right;

    private String op;

    public BinaryOpExpr(VerdictContext vc, Expr left, Expr right, String op) {
        super(vc);
        this.left = left;
        this.right = right;
        this.op = op;
    }

    public static BinaryOpExpr from(VerdictContext vc, Expr left, Expr right, String op) {
        return new BinaryOpExpr(vc, left, right, op);
    }

    public Expr getLeft() {
        return left;
    }

    public Expr getRight() {
        return right;
    }

    public String getOp() {
        return op;
    }

    @Override
    public String toString() {
        return String.format("(%s %s %s)", left.toString(), op, right.toString());
    }

    @Override
    public <T> T accept(ExprVisitor<T> v) {
        return v.call(this);
    }

    @Override
    public boolean isagg() {
        return left.isagg() || right.isagg();
    }

    @Override
    public Expr withTableSubstituted(String newTab) {
        return new BinaryOpExpr(vc, left.withTableSubstituted(newTab), right.withTableSubstituted(newTab), op);
    }

    @Override
    public Expr withNewTablePrefix(String newPrefix) {
        return new BinaryOpExpr(vc, left.withNewTablePrefix(newPrefix), right.withNewTablePrefix(newPrefix), op);
    }

    @Override
    public String toSql() {
        return String.format("(%s %s %s)", left.toSql(), op, right.toSql());
    }

    @Override
    public int hashCode() {
        return left.hashCode() + right.hashCode() + op.hashCode();
    }

    @Override
    public boolean equals(Expr o) {
        if (o instanceof BinaryOpExpr) {
            return getOp().equals(((BinaryOpExpr) o).getOp()) && getLeft().equals(((BinaryOpExpr) o).getLeft())
                    && getRight().equals(((BinaryOpExpr) o).getRight());
        }
        return false;
    }
}
