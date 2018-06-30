package org.verdictdb.sql.syntax;

public class MysqlSyntax implements SqlSyntax {
  @Override
  public String getQuoteString() {
    return "'";
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
    return "show schemas";
  }

  @Override
  public String getTableCommand(String schema) {
    return "show tables in " + schema;
  }

  @Override
  public String getColumnsCommand(String schema, String table) {
    return "show columns in " + table + " in " + schema;
  }

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
  public String getPartitionCommand(String schema, String table) {
    return "SELECT DISTINCT PARTITION_EXPRESSION FROM INFORMATION_SCHEMA.PARTITIONS " +
        "WHERE TABLE_NAME='" + table +"' AND TABLE_SCHEMA='" + schema + "'";
  }
}
