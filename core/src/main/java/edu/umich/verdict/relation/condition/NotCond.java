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

public class NotCond extends Cond {

    private Cond cond;

    public NotCond(Cond cond) {
        this.cond = cond;
    }

    public static NotCond from(Cond cond) {
        return new NotCond(cond);
    }

    public Cond getCond() {
        return cond;
    }

    @Override
    public String toString() {
        return String.format("NOT (%s)", cond);
    }

    @Override
    public Cond withTableSubstituted(String newTab) {
        return new NotCond(cond.withTableSubstituted(newTab));
    }

    @Override
    public Cond withNewTablePrefix(String newPrefix) {
        return new NotCond(cond.withNewTablePrefix(newPrefix));
    }

    @Override
    public String toSql() {
        return String.format("NOT (%s)", cond.toSql());
    }

    @Override
    public boolean equals(Cond o) {
        if (o instanceof NotCond) {
            return cond.equals(o);
        }
        return false;
    }
}
