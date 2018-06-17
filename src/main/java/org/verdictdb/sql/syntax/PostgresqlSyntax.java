package org.verdictdb.sql.syntax;

public class PostgresqlSyntax implements SyntaxAbstract {

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
    return "random()";
  }

  @Override
  public String getSchemaCommand() {
    return "select schema_name from information_schema.schemata";
  }

  @Override
  public String getTableCommand(String schema) {
    return "SELECT table_name FROM information_schema.tables " + "WHERE table_schema = '" + schema +"'";
  }

  @Override
  public String getColumnsCommand(String schema, String table) {
    return "select column_name, data_type " +
        "from INFORMATION_SCHEMA.COLUMNS where table_name = '" + table + "' and table_schema = '" + schema + "'";
  }

  @Override
  public String getPartitionCommand(String schema, String table) {
    return "select inhrelid::regclass " +
        "from pg_inherits " +
        "where inhparent = '" + schema + "." + table + "'::regclass";
  }
}