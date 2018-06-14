package org.verdictdb.sql.syntax;

public class PostgresqlSyntax implements SyntaxAbstract {

  // The column name that stored meta information in the original database

  final private String schemaNameColumnName = "schema_name";

  final private String TableNameColumnName = "table_name";

  final private String ColumnNameColumnName = "column_name";

  final private String ColumnTypeColumnName = "data_type";

  @Override
  public String getSchemaNameColumnName() {
    return schemaNameColumnName;
  }

  @Override
  public String getTableNameColumnName() {
    return TableNameColumnName;
  }

  @Override
  public String getColumnNameColumnName() {
    return ColumnNameColumnName;
  }

  @Override
  public String getColumnTypeColumnName() {
    return ColumnTypeColumnName;
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
    return "SELECT * FROM information_schema.tables " + "WHERE table_schema = '" + schema +"'";
  }

  @Override
  public String getColumnsCommand(String schema, String table) {
    return "select column_name, data_type " +
        "from INFORMATION_SCHEMA.COLUMNS where table_name = '" + table + "' and table_schema = '" + schema + "'";
  }

  @Override
  public String getPartitionCommand(String schema, String table) {
    return "select relname " +
        "from pg_inherits i " +
        "join pg_class c on c.oid = inhrelid " +
        "where inhparent = '" + schema + "." + table + "'::regclass";
  }
}