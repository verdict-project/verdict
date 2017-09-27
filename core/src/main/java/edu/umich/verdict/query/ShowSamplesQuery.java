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
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.relation.ExactRelation;
import edu.umich.verdict.relation.Relation;
import edu.umich.verdict.relation.SingleRelation;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public class ShowSamplesQuery extends SelectQuery {

    public ShowSamplesQuery(VerdictContext vc, String q) {
        super(vc, q);
    }

    @Override
    public void compute() throws VerdictException {
        VerdictSQLParser p = StringManipulations.parserOf(queryString);
        VerdictSQLBaseVisitor<String> visitor = new VerdictSQLBaseVisitor<String>() {
            @Override
            public String visitShow_samples_statement(VerdictSQLParser.Show_samples_statementContext ctx) {

                String database = null;
                if (ctx.database != null) {
                    database = ctx.database.getText();
                }
                return database;
            }
        };
        
        String database = visitor.visit(p.show_samples_statement());
        
        database = (database != null) ? database
                : ((vc.getCurrentSchema().isPresent()) ? vc.getCurrentSchema().get() : null);

        if (database == null) {
            VerdictLogger.info("No table specified; cannot show samples");
        } else {
            String metaDatabaseName = vc.getMeta().metaCatalogForDataCatalog(database);
            
            if (!vc.getMeta().getDatabases().contains(database)) {
                VerdictLogger.info("The specified database does not exist.");
                return;
            } else if (!vc.getMeta().getDatabases().contains(metaDatabaseName)){
                VerdictLogger.info(String.format("No samples have been created for the database: %s.", database));
                return;
            } else if (!vc.getMeta().getTables(metaDatabaseName).contains(vc.getMeta().getMetaNameTableForOriginalSchema(database).getTableName())) {
                VerdictLogger.info(String.format("No samples have been created for the database: %s.", database));
                return;
            }
            

            ExactRelation nameTable = SingleRelation.from(vc, vc.getMeta().getMetaNameTableForOriginalSchema(database));
            nameTable.setAlias("s");
            ExactRelation sizeTable = SingleRelation.from(vc, vc.getMeta().getMetaSizeTableForOriginalSchema(database));
            sizeTable.setAlias("t");

            Relation info = nameTable.join(sizeTable, "s.sampleschemaaname = t.schemaname AND s.sampletablename = t.tablename")
                    .select("s.originaltablename AS `original_table`,"
                            + " s.sampletype AS `sample_type`,"
                            + " t.schemaname AS `sample_schema_name`,"
                            + " s.sampletablename AS `sample_table_name`,"
                            + " s.samplingratio AS `sampling_ratio`,"
                            + " s.columnnames AS `on_columns`,"
                            + " t.originaltablesize AS `original_table_size`,"
                            + " t.samplesize AS `sample_table_size`")
                    .orderby("`original_table`, `sample_type`, `sampling_ratio`, `on_columns`");

            if (vc.getDbms().isJDBC()) {
                rs = info.collectResultSet();
            } else if (vc.getDbms().isSpark()) {
                df = info.collectDataFrame();
            }
        }
    }

}
