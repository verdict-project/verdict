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

package edu.umich.verdict.query;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

public class CreateTableAsSelectQuery extends Query {

    public CreateTableAsSelectQuery(VerdictContext vc, String q) {
        super(vc, q);
    }

    @Override
    public void compute() throws VerdictException {
        VerdictLogger.error(this, "Not supported.");
        // String rewrittenQuery = rewriteQuery(queryString);
        //
        // VerdictLogger.debug(this, "The input query was rewritten to:");
        // VerdictLogger.debugPretty(this, rewrittenQuery, " ");
        //
        // vc.getDbms().executeUpdate(rewrittenQuery);
        // return null;
    }

    // private String rewriteQuery(final String query) {
    // VerdictSQLParser p = StringManupulations.parserOf(queryString);
    //
    // SelectStatementBaseRewriter visitor = new SelectStatementBaseRewriter(query)
    // {
    // @Override
    // public String
    // visitCreate_table_as_select(VerdictSQLParser.Create_table_as_selectContext
    // ctx) {
    // StringBuilder sql = new StringBuilder();
    // sql.append("CREATE TABLE");
    // if (ctx.IF() != null) sql.append(" IF NOT EXISTS");
    // sql.append(String.format(" %s AS \n", ctx.table_name().getText()));
    // AnalyticSelectStatementRewriter selectVisitor = new
    // AnalyticSelectStatementRewriter(vc, query);
    // selectVisitor.setIndentLevel(2);
    // sql.append(selectVisitor.visit(ctx.select_statement()));
    // return sql.toString();
    // }
    // };
    //
    // return visitor.visit(p.create_table_as_select());
    // }
}
