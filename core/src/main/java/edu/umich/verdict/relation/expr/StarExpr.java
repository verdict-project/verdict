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

public class StarExpr extends Expr {
    
    TableNameExpr tab = null;
    
    public TableNameExpr getTab() {
        return tab;
    }

    public void setTab(TableNameExpr tab) {
        this.tab = tab;
    }

    public StarExpr(TableNameExpr t) {
        super(VerdictContext.dummyContext());
        tab = t;
    }

    public StarExpr() {
        super(VerdictContext.dummyContext());
    }

    @Override
    public String toString() {
        if (tab == null) {
            return "*";
        } else {
            return tab.toString() + ".*";
        }
    }

    @Override
    public <T> T accept(ExprVisitor<T> v) {
        return v.call(this);
    }

    @Override
    public Expr withTableSubstituted(String newTab) {
        return this;
    }

    @Override
    public Expr withNewTablePrefix(String newPrefix) {
        return this;
    }

    @Override
    public String toSql() {
        return toString();
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public boolean equals(Expr o) {
        if (o instanceof StarExpr) {
            return true;
        }
        return false;
    }

}
