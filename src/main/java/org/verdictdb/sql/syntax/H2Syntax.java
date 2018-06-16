package org.verdictdb.sql.syntax;

public class H2Syntax implements SyntaxAbstract {

  // The column index that stored meta information in the original database

  @Override
  public int getSchemaNameColumnIndex() {
    return 0;
  }

  @Override
  public int getTableNameColumnIndex() {
    return 0;
  }

  @Override
  public int getColumnNameColumnIndex() {
    return 0;
  }

  @Override
  public int getColumnTypeColumnIndex() {
    return 1;
  }

  @Override
  public String getQuoteString() {
    return "";
  }

  @Override
  public void dropTable(String schema, String tablename) { }

  @Override
  public boolean doesSupportTablePartitioning() {
    return false;
  }

  @Override
  public String randFunction() {
    return "rand()";
  }

  @Override
  public String getSchemaCommand() {
    return "show schemas";
  }

  @Override
  public String getTableCommand(String schema) {
    return "show tables from " + schema;
  }

  @Override
  public String getColumnsCommand(String schema, String table) {
    return "show columns from " + table + " from " + schema;
  }

  @Override
  public String getPartitionCommand(String schema, String table) {
    return null;
  }

}
