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

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.relation.ExactRelation;

public class AndCond extends Cond {

    private Cond left;

    private Cond right;

    public AndCond(Cond left, Cond right) {
        this.left = left;
        this.right = right;
    }

    public static AndCond from(Cond left, Cond right) {
        return new AndCond(left, right);
    }

    public Cond getLeft() {
        return left;
    }

    public Cond getRight() {
        return right;
    }

    @Override
    public Cond accept(CondModifier v) {
        return from(left.accept(v), right.accept(v));
    }

    @Override
    public String toString() {
        return String.format("(%s) AND (%s)", left, right);
    }

    @Override
    public Pair<Cond, Pair<ExactRelation, ExactRelation>> searchForJoinCondition(List<ExactRelation> tableSources) {
        Pair<Cond, Pair<ExactRelation, ExactRelation>> j = null;
        j = left.searchForJoinCondition(tableSources);
        if (j == null)
            j = right.searchForJoinCondition(tableSources);
        return j;
    }

    // dyoon: Extracts join conditions from the current AndCond.
    // This looks a duplicate of searchForJoinCondition, but its implementation under CompCond
    // differs from that of searchForJoinCondition.
    @Override
    public Pair<Cond, Pair<ExactRelation, ExactRelation>> extractJoinCondition(List<ExactRelation> tableSources) {
        Pair<Cond, Pair<ExactRelation, ExactRelation>> j = null;
        j = left.extractJoinCondition(tableSources);
        if (j == null)
            j = right.extractJoinCondition(tableSources);
        return j;
    }

    @Override
    public Cond remove(Cond j) {
        if (left.equals(j)) {
            return right;
        } else if (right.equals(j)) {
            return left;
        } else {
            return from(left.remove(j), right.remove(j));
        }

    }

    @Override
    public Cond withTableSubstituted(String newTab) {
        return new AndCond(left.withTableSubstituted(newTab), right.withTableSubstituted(newTab));
    }

    @Override
    public Cond withNewTablePrefix(String newPrefix) {
        return new AndCond(left.withNewTablePrefix(newPrefix), right.withNewTablePrefix(newPrefix));
    }

    @Override
    public String toSql() {
        return String.format("(%s) AND (%s)", left.toSql(), right.toSql());
    }

    @Override
    public boolean equals(Cond o) {
        if (o instanceof AndCond) {
            return getLeft().equals(((AndCond) o).getLeft()) && getRight().equals(((AndCond) o).getRight());
        }
        return false;
    }
}
