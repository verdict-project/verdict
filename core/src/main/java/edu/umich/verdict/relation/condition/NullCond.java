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

/**
 * NULL or NOT NULL
 * @author Yongjoo Park
 *
 */
public class NullCond extends Cond {
    
    boolean isNull = true;
    
    public boolean isNull() {
        return isNull;
    }

    public void setNull(boolean isNull) {
        this.isNull = isNull;
    }
    
    public NullCond() {
        this(true);
    }

    public NullCond (boolean isNull) {
        this.isNull = isNull;
    }

    @Override
    public String toString() {
        return (isNull)? "NULL" : "NOT NULL";
    }

    @Override
    public Cond withTableSubstituted(String newTab) {
        return this;
    }

    @Override
    public Cond withNewTablePrefix(String newPrefix) {
        return this;
    }

    @Override
    public String toSql() {
        return toString();
    }

    @Override
    public boolean equals(Cond o) {
        if (o instanceof NullCond) {
            if (isNull() == ((NullCond) o).isNull()) {
                return true;
            }
        }
        return false;
    }

}
