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
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.relation.ExactRelation;
import edu.umich.verdict.relation.Relation;

public class SubqueryExpr extends Expr {

    private Relation subquery;

    public SubqueryExpr(VerdictContext vc, Relation subquery) {
        super(vc);
        this.subquery = subquery;
    }

    public static SubqueryExpr from(VerdictContext vc, Relation r) {
        return new SubqueryExpr(vc, r);
    }

    public static SubqueryExpr from(VerdictContext vc, VerdictSQLParser.Subquery_expressionContext ctx) {
        return from(vc, ctx.subquery());
    }
    
    public static SubqueryExpr from(VerdictContext vc, VerdictSQLParser.SubqueryContext ctx) {
        return from(vc, ExactRelation.from(vc, ctx.select_statement()));
    }

    public Relation getSubquery() {
        return subquery;
    }

    @Override
    public <T> T accept(ExprVisitor<T> v) {
        return v.call(this);
    }

    @Override
    public String toString() {
        return "(" + subquery.toString() + ")";
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
        return "(" + subquery.toSql() + ")";
    }

    @Override
    public int hashCode() {
        return subquery.hashCode();
    }

    @Override
    public boolean equals(Expr o) {
        if (o instanceof SubqueryExpr) {
            return ((SubqueryExpr) o).getSubquery().equals(((SubqueryExpr) o).getSubquery());
        }
        return false;
    }
}
