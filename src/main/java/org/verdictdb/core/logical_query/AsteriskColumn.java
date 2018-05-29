package org.verdictdb.core.logical_query;

public class AsteriskColumn implements UnnamedColumn, SelectItem {

  String tablename = null;

  public AsteriskColumn() { };

  public AsteriskColumn(String tablename) {
    this.tablename = tablename;
  }

  public String getTablename() {
    return tablename;
  }

  public void setTablename(String tablename) {
    this.tablename = tablename;
  }
}
