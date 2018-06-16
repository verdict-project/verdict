package org.verdictdb.sql.syntax;

public class HiveSyntax implements SyntaxAbstract {

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
    return "`";
  }

  @Override
  public void dropTable(String schema, String tablename) {

  }

  @Override
  public boolean doesSupportTablePartitioning() {
    return true;
  }

  @Override
  public String randFunction() {
    return "rand()";
  }

  @Override
  public String getSchemaCommand() {
    return "SHOW DATABASES";
  }

  @Override
  public String getTableCommand(String schema) {
    return "SHOW TABLES IN " + schema;
  }

  @Override
  public String getColumnsCommand(String schema, String table) {
    return "DESCRIBE " + schema + "." + table;
  }

  @Override
  public String getPartitionCommand(String schema, String table) {
    return "SHOW PARTITIONS " + schema + "." + table;
  }
}
