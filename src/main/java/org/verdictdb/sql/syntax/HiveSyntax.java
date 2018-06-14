package org.verdictdb.sql.syntax;

public class HiveSyntax implements SyntaxAbstract {

  // The column name that stored meta information in the original database

  final private String schemaNameColumnName = "database_name";

  final private String TableNameColumnName = "tab_name";

  final private String ColumnNameColumnName = "col_name";

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
