package org.verdictdb.sqlsyntax;

public interface SqlSyntax {

  public boolean doesSupportTablePartitioning();

  public void dropTable(String schema, String tablename);

  // The column index that stored meta information in the original database
  public int getColumnNameColumnIndex();

  public String getColumnsCommand(String schema, String table);

  // The column index that stored meta information in the original database
  public int getColumnTypeColumnIndex();

  public String getPartitionByInCreateTable();

  public String getPartitionCommand(String schema, String table);

  public String getQuoteString();

  public String getSchemaCommand();

  public int getSchemaNameColumnIndex();

  public String getTableCommand(String schema);

  // The column index that stored meta information in the original database
  public int getTableNameColumnIndex();

  public String randFunction();
  
  public boolean isAsRequiredBeforeSelectInCreateTable();
}
