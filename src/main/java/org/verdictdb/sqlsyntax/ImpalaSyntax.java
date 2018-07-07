package org.verdictdb.sqlsyntax;

public class ImpalaSyntax implements SqlSyntax {

  @Override
  public boolean doesSupportTablePartitioning() {
    return true;
  }

  @Override
  public void dropTable(String schema, String tablename) {

  }

  @Override
  public int getColumnNameColumnIndex() {
    return 0;
  }

  @Override
  public String getColumnsCommand(String schema, String table) {
    return "DESCRIBE " + schema + "." + table;
  }

  @Override
  public int getColumnTypeColumnIndex() {
    return 1;
  }

  @Override
  public String getPartitionByInCreateTable() {
    return "partitioned by";
  }

  @Override
  public String getPartitionCommand(String schema, String table) {
    return "SHOW PARTITIONS " + schema + "." + table;
  }

  @Override
  public String getQuoteString() {
    return "`";
  }

  @Override
  public String getSchemaCommand() {
    return "SHOW DATABASES";
  }

  @Override
  public int getSchemaNameColumnIndex() {
    return 0;
  }

  @Override
  public String getTableCommand(String schema) {
    return "SHOW TABLES IN " + schema;
  }

  @Override
  public int getTableNameColumnIndex() {
    return 0;
  }

  @Override
  public String randFunction() {
    return "rand()";
  }
  
  @Override
  public boolean isAsRequiredBeforeSelectInCreateTable() {
    return true;
  }
}
