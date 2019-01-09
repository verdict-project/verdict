/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.sqlreader;

import org.verdictdb.core.sqlobject.CreateScrambleQuery;
import org.verdictdb.parser.VerdictSQLParser.Create_scramble_statementContext;
import org.verdictdb.parser.VerdictSQLParser.Table_nameContext;

public class ScramblingQueryGenerator {

  public CreateScrambleQuery visit(Create_scramble_statementContext create_scramble_statement) {
    Table_nameContext scrambled_table = create_scramble_statement.scrambled_table;
    Table_nameContext original_table = create_scramble_statement.original_table;

    String newSchema = stripQuote(scrambled_table.schema.getText());
    String newTable = stripQuote(scrambled_table.table.getText());
    String originalSchema = stripQuote(original_table.schema.getText());
    String originalTable = stripQuote(original_table.table.getText());
    String method = create_scramble_statement.METHOD().getText();
    String sizeInString = create_scramble_statement.SIZE().getText();
    double size = Math.min(Double.valueOf(sizeInString), 100.0) / 100.0;
    long blocksize = Long.parseLong(create_scramble_statement.blocksize.getText());
    String hashColumnName = stripQuote(create_scramble_statement.hash_column.getText());

    CreateScrambleQuery query =
        new CreateScrambleQuery(
            newSchema,
            newTable,
            originalSchema,
            originalTable,
            method,
            size,
            blocksize,
            hashColumnName,
            null);

    return query;
  }

  private String stripQuote(String expr) {
    return expr.replace("\"", "").replace("`", "");
  }
}
