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
import edu.umich.verdict.dbms.DbmsJDBC;
//import edu.umich.verdict.dbms.DbmsSpark;
import edu.umich.verdict.dbms.DbmsSpark2;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public class ShowTablesQuery extends SelectQuery {

    public ShowTablesQuery(VerdictContext vc, String q) {
        super(vc, q);
    }

    @Override
    public void compute() throws VerdictException {
        VerdictSQLParser p = StringManipulations.parserOf(queryString);
        VerdictSQLBaseVisitor<String> visitor = new VerdictSQLBaseVisitor<String>() {
            private String schemaName = null;

            protected String defaultResult() {
                return schemaName;
            }

            @Override
            public String visitShow_tables_statement(VerdictSQLParser.Show_tables_statementContext ctx) {
                if (ctx.schema != null) {
                    schemaName = ctx.schema.getText();
                }
                return schemaName;
            }
        };
        String schema = visitor.visit(p.show_tables_statement());
        schema = (schema != null) ? schema : ((vc.getCurrentSchema().isPresent()) ? vc.getCurrentSchema().get() : null);

        if (schema == null) {
            VerdictLogger.info("No schema specified; cannot show tables.");
            return;
        } else {
            if (vc.getDbms().isJDBC()) {
                rs = ((DbmsJDBC) vc.getDbms()).getTablesInResultSet(schema);
//            } else if (vc.getDbms().isSpark()) {
//                df = ((DbmsSpark) vc.getDbms()).getTablesInDataFrame(schema);
            } else if (vc.getDbms().isSpark2()) {
                ds = ((DbmsSpark2) vc.getDbms()).getTablesInDataset(schema);
            }
        }
    }

}
