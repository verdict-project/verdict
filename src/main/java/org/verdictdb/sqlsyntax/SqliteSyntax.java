package org.verdictdb.sqlsyntax;

public class SqliteSyntax extends SqlSyntax {

  @Override
  public boolean doesSupportTablePartitioning() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void dropTable(String schema, String tablename) {
    // TODO Auto-generated method stub

  }

  @Override
  public int getColumnNameColumnIndex() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getColumnsCommand(String schema, String table) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getColumnTypeColumnIndex() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getPartitionByInCreateTable() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getPartitionCommand(String schema, String table) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getQuoteString() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getSchemaCommand() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getSchemaNameColumnIndex() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getTableCommand(String schema) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getTableNameColumnIndex() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String randFunction() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isAsRequiredBeforeSelectInCreateTable() {
    // TODO Auto-generated method stub
    return false;
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
