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

public class OrCond extends Cond {

    private Cond left;

    private Cond right;

    public OrCond(Cond left, Cond right) {
        this.left = left;
        this.right = right;
    }

    public static OrCond from(Cond left, Cond right) {
        return new OrCond(left, right);
    }

    public Cond getLeft() {
        return left;
    }

    public void setLeft(Cond left) {
        this.left = left;
    }

    public Cond getRight() {
        return right;
    }

    public void setRight(Cond right) {
        this.right = right;
    }

    @Override
    public Cond accept(CondModifier v) {
        return from(left.accept(v), right.accept(v));
    }

    @Override
    public String toString() {
        return String.format("(%s) OR (%s)", left.toString(), right.toString());
    }

    @Override
    public Cond withTableSubstituted(String newTab) {
        return new OrCond(left.withTableSubstituted(newTab), right.withTableSubstituted(newTab));
    }

    @Override
    public Cond withNewTablePrefix(String newPrefix) {
        return new OrCond(left.withNewTablePrefix(newPrefix), right.withNewTablePrefix(newPrefix));
    }

    @Override
    public String toSql() {
        return String.format("(%s) OR (%s)", left.toSql(), right.toSql());
    }

    @Override
    public boolean equals(Cond o) {
        if (o instanceof OrCond) {
            return getLeft().equals(((OrCond) o).getLeft()) && getRight().equals(((OrCond) o).getRight());
        }
        return false;
    }
}
