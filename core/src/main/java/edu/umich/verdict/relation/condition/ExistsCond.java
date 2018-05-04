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
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.SubqueryExpr;
import edu.umich.verdict.util.VerdictLogger;

public class ExistsCond extends Cond {

    private Expr subquery = null;

    public ExistsCond(Expr subquery) {
        this.subquery = subquery;
    }
    public Expr getSubquery() {
        return subquery;
    }

    public static ExistsCond from(VerdictContext vc, VerdictSQLParser.Exists_predicateContext ctx) {
        if(ctx.subquery() == null) {
            VerdictLogger.error("Exists should be followed by a subquery");
        }
        Expr subquery = SubqueryExpr.from(vc,ctx.subquery());
        return new ExistsCond(subquery);
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(" EXISTS (");
        sql.append(subquery.toSql());
        sql.append(")");
        return sql.toString();
    }


    @Override
    public boolean equals(Cond o) {
        if (o instanceof ExistsCond) {
            return subquery.equals(((ExistsCond) o).subquery);
        }
        return false;
    }

    @Override
    public Cond withTableSubstituted(String newTab) {
        return this;
    }

    @Override
    public Cond withNewTablePrefix(String newPrefix) {
        return this;
    }

}
