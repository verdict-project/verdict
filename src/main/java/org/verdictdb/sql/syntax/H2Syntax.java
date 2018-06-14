package org.verdictdb.sql.syntax;

public class H2Syntax implements SyntaxAbstract {

  // The column name that stored meta information in the original database

  final private String schemaNameColumnName = "SCHEMA_NAME";

  final private String TableNameColumnName = "TABLE_NAME";

  final private String ColumnNameColumnName = "FIELD";

  final private String ColumnTypeColumnName = "TYPE";

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
