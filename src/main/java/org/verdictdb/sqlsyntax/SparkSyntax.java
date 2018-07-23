package org.verdictdb.sqlsyntax;

public class SparkSyntax extends SqlSyntax {

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
    return "DESCRIBE " + quoteName(schema) + "." + quoteName(table);
  }

  @Override
  public int getColumnTypeColumnIndex() {
    return 1;
  }
  
  @Override
  public String getFallbackDefaultSchema() {
    return "default";
  }

  
  @Override
  public String getPartitionByInCreateTable() {
    return "using parquet partitioned by";
  }

  /**
   * This command also returns partition information if exists.
   */
  @Override
  public String getPartitionCommand(String schema, String table) {
    return "DESCRIBE " + quoteName(schema) + "." + quoteName(table);
//    return "SHOW PARTITIONS " + quoteName(schema) + "." + quoteName(table);
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
    return "SHOW TABLES IN " + quoteName(schema);
  }

  @Override
  public int getTableNameColumnIndex() {
    return 1;
  }

  @Override
  public String randFunction() {
    return "rand()";
  }
  
  @Override
  public boolean isAsRequiredBeforeSelectInCreateTable() {
    return true;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    return true;
  }
  
}
