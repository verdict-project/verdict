package org.verdictdb.sqlsyntax;

public interface SqlSyntax {

  public String getQuoteString();

  public void dropTable(String schema, String tablename);

  public boolean doesSupportTablePartitioning();

  public String randFunction();

  public String getSchemaCommand();

  public String getTableCommand(String schema);

  public String getColumnsCommand(String schema, String table);

  public int getSchemaNameColumnIndex();

  public int getTableNameColumnIndex();

  public int getColumnNameColumnIndex();

  public int getColumnTypeColumnIndex();

  public String getPartitionCommand(String schema, String table);
}
