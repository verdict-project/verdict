package org.verdictdb.sqlsyntax;

public class MysqlSyntax implements SqlSyntax {
  
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
    return "show columns in " + table + " in " + schema;
  }

  @Override
  public int getColumnTypeColumnIndex() {
    return 1;
  }

  @Override
  public String getPartitionByInCreateTable() {
    return "partition by key";
  }

  @Override
  public String getPartitionCommand(String schema, String table) {
    return "SELECT DISTINCT PARTITION_EXPRESSION FROM INFORMATION_SCHEMA.PARTITIONS " +
        "WHERE TABLE_NAME='" + table +"' AND TABLE_SCHEMA='" + schema + "'";
  }

  @Override
  public String getQuoteString() {
    // https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
    return "`";
  }

  @Override
  public String getSchemaCommand() {
    return "show schemas";
  }

  @Override
  public int getSchemaNameColumnIndex() {
    return 0;
  }

  @Override
  public String getTableCommand(String schema) {
    return "show tables in " + schema;
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
    return false;
  }
}
