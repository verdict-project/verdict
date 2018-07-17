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
    
    CreateScrambleQuery query = new CreateScrambleQuery(
        newSchema, newTable, originalSchema, originalTable, method, size);
    
    return query;
  }
  
  private String stripQuote(String expr) {
    return expr.replace("\"", "").replace("`", "");
  }

}
