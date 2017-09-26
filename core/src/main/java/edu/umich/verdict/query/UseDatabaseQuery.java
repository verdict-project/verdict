/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.util.StringManipulations;

public class UseDatabaseQuery extends Query {

    public UseDatabaseQuery(VerdictContext vc, String q) {
        super(vc, q);
    }

    @Override
    public void compute() throws VerdictException {
        VerdictSQLParser p = StringManipulations.parserOf(queryString);

        VerdictSQLBaseVisitor<String> visitor = new VerdictSQLBaseVisitor<String>() {
            private String schemaName;

            protected String defaultResult() {
                return schemaName;
            }

            @Override
            public String visitUse_statement(VerdictSQLParser.Use_statementContext ctx) {
                schemaName = ctx.database.getText();
                return schemaName;
            }
        };

        String schema = visitor.visit(p.use_statement());
        vc.getDbms().changeDatabase(schema);
        vc.getMeta().refreshSampleInfoIfNeeded(schema);
        vc.getMeta().refreshTables(schema);
    }
}
