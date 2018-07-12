package org.verdictdb.sqlsyntax;

public abstract class SqlSyntax {

  public abstract boolean doesSupportTablePartitioning();

  public abstract void dropTable(String schema, String tablename);

  // The column index that stored meta information in the original database
  public abstract int getColumnNameColumnIndex();

  public abstract String getColumnsCommand(String schema, String table);

  // The column index that stored meta information in the original database
  public abstract int getColumnTypeColumnIndex();

  public abstract String getPartitionByInCreateTable();

  public abstract String getPartitionCommand(String schema, String table);

  public abstract String getQuoteString();

  public abstract String getSchemaCommand();

  public abstract int getSchemaNameColumnIndex();

  public abstract String getTableCommand(String schema);

  // The column index that stored meta information in the original database
  public abstract int getTableNameColumnIndex();

  public abstract String randFunction();
  
  public abstract boolean isAsRequiredBeforeSelectInCreateTable();

  public String getStddevPopulationFunctionName() {
    return "stddev_pop";
  }
  
  public String quoteName(String name) {
    String quoteString = getQuoteString();
    return quoteString + name + quoteString;
  }
}
