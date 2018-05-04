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

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.parser.VerdictSQLParser.Like_predicateContext;
import edu.umich.verdict.relation.expr.Expr;

public class LikeCond extends Cond {

    private Expr left;

    private Expr right;

    private boolean not;

    public Expr getLeft() {
        return left;
    }

    public void setLeft(Expr left) {
        this.left = left;
    }

    public Expr getRight() {
        return right;
    }

    public void setRight(Expr right) {
        this.right = right;
    }

    public boolean isNot() {
        return not;
    }

    public void setNotExists(boolean notExists) {
        this.not = notExists;
    }

    public LikeCond(Expr left, Expr right, boolean not) {
        this.left = left;
        this.right = right;
        this.not = not;
    }

    public static LikeCond from(VerdictContext vc, Like_predicateContext ctx) {
        Expr left = Expr.from(vc, ctx.expression(0));
        Expr right = Expr.from(vc, ctx.expression(1));
        boolean not = (ctx.NOT() != null) ? true : false;

        return new LikeCond(left, right, not);
    }

    @Override
    public Cond withTableSubstituted(String newTab) {
        return new LikeCond(getLeft().withTableSubstituted(newTab), getRight().withTableSubstituted(newTab), isNot());
    }

    @Override
    public Cond withNewTablePrefix(String newPrefix) {
        return new LikeCond(getLeft().withNewTablePrefix(newPrefix), getRight().withNewTablePrefix(newPrefix), isNot());
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(left.toSql());
        if (isNot()) {
            sql.append(" NOT");
        }
        sql.append(" LIKE ");
        sql.append(right.toSql());
        return sql.toString();
    }

    @Override
    public boolean equals(Cond o) {
        if (o instanceof LikeCond) {
            return getLeft().equals(((LikeCond) o).getLeft()) && getRight().equals(((LikeCond) o).getRight())
                    && (isNot() == ((LikeCond) o).isNot());
        }
        return false;
    }

}
