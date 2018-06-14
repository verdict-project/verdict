package org.verdictdb.sql.syntax;

public interface SyntaxAbstract {

  public String getQuoteString();

  public void dropTable(String schema, String tablename);

  public boolean doesSupportTablePartitioning();

  public String randFunction();

  public String getSchemaCommand();

  public String getTableCommand(String schema);

  public String getColumnsCommand(String schema, String table);

  public String getSchemaNameColumnName();

  public String getTableNameColumnName();

  public String getColumnNameColumnName();

  public String getColumnTypeColumnName();

  public String getPartitionCommand(String schema, String table);
}
