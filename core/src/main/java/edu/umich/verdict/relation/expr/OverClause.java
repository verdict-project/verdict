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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.parser.VerdictSQLParser.ExpressionContext;
import edu.umich.verdict.parser.VerdictSQLParser.Over_clauseContext;
import edu.umich.verdict.parser.VerdictSQLParser.Partition_by_clauseContext;
import edu.umich.verdict.util.StringManipulations;

public class OverClause {

    protected List<Expr> partitionBy;

    public OverClause() {
        this.partitionBy = new ArrayList<Expr>();
    }

    public OverClause(List<Expr> partitionBy) {
        this.partitionBy = partitionBy;
    }

    public static OverClause from(VerdictContext vc, String partitionByInString) {
        VerdictSQLParser p = StringManipulations.parserOf(partitionByInString);
        return from(vc, p.over_clause());
    }

    @Override
    public String toString() {
        if (partitionBy.size() > 0) {
            return String.format("OVER (partition by %s)", Joiner.on(", ").join(partitionBy));
        } else {
            return "OVER ()";
        }
    }

    public static OverClause from(VerdictContext vc, Over_clauseContext over_clause) {
        List<Expr> exprs = new ArrayList<Expr>();
        if (over_clause.partition_by_clause() != null) {
            Partition_by_clauseContext pctx = over_clause.partition_by_clause();
            for (ExpressionContext ectx : pctx.expression_list().expression()) {
                exprs.add(Expr.from(vc, ectx));
            }
        }
        return new OverClause(exprs);
    }

}
