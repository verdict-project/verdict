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

import edu.umich.verdict.relation.expr.Expr;

public class IsCond extends Cond {

    private Expr left;

    private Cond right;

    public Expr getLeft() {
        return left;
    }

    public void setLeft(Expr left) {
        this.left = left;
    }

    public Cond getRight() {
        return right;
    }

    public void setRight(Cond right) {
        this.right = right;
    }

    public IsCond(Expr left, Cond right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public String toString() {
        return String.format("(%s IS %s)", left, right);
    }

    @Override
    public Cond withTableSubstituted(String newTab) {
        return new IsCond(left.withTableSubstituted(newTab), right.withTableSubstituted(newTab));
    }

    @Override
    public Cond withNewTablePrefix(String newPrefix) {
        return new IsCond(left.withNewTablePrefix(newPrefix), right.withNewTablePrefix(newPrefix));
    }

    @Override
    public String toSql() {
        return String.format("(%s IS %s)", left.toSql(), right.toSql());
    }

    @Override
    public boolean equals(Cond o) {
        if (o instanceof IsCond) {
            return getLeft().equals(((IsCond) o).getLeft()) && getRight().equals(((IsCond) o).getRight());
        }
        return false;
    }

}
