package org.verdictdb.sqlsyntax;

public class PostgresqlSyntax implements SqlSyntax {
  
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
    return "\"";
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
    return "select partattrs from pg_partitioned_table join pg_class on pg_class.relname='" + table + "' " +
        "and pg_class.oid = pg_partitioned_table.partrelid join information_schema.tables " +
        "on table_schema='" + schema + "' and table_name = '" + table + "'";
  }

  @Override
  public String getPartitionByInCreateTable() {
    // not implemented yet
    // postgres has some complication that sub-partition tables must be created individually.
    return null;
  }

  @Override
  public boolean isAsRequiredBeforeSelectInCreateTable() {
    return true;
  }
}