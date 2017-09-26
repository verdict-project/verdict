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
import edu.umich.verdict.util.VerdictLogger;

public class RefreshQuery extends Query {

    public RefreshQuery(VerdictContext vc, String q) {
        super(vc, q);
    }

    @Override
    public void compute() throws VerdictException {
        VerdictSQLParser p = StringManipulations.parserOf(queryString);
        VerdictSQLBaseVisitor<String> visitor = new VerdictSQLBaseVisitor<String>() {
            @Override
            public String visitRefresh_statement(VerdictSQLParser.Refresh_statementContext ctx) {
                String schema = null;
                if (ctx.schema != null) {
                    schema = ctx.schema.getText();
                }
                return schema;
            }
        };
        String schema = visitor.visit(p.refresh_statement());
        schema = (schema != null)? schema : ( (vc.getCurrentSchema().isPresent())? vc.getCurrentSchema().get() : null );

        vc.getMeta().clearSampleInfo();
        
        if (schema != null) {
            vc.getMeta().refreshSampleInfo(schema);
//            vc.getMeta().refreshTables(schema);

        } else {
            String msg = "No schema selected. No refresh done.";
            VerdictLogger.error(msg);
            throw new VerdictException(msg);
        }
    }

}
