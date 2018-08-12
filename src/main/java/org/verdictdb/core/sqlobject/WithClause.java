package org.verdictdb.core.sqlobject;

public class WithClause implements SqlConvertible {
  
  private static final long serialVersionUID = -3295224306523301269L;
  
  private String tableName;
  
  private SelectQuery selectQuery;
  
  public WithClause(String tableName, SelectQuery selectQuery) {
    this.tableName = tableName;
    this.selectQuery = selectQuery;
  }

  public SelectQuery getSelectQuery() {
    return selectQuery;
  }

  public String getTableName() {
    return tableName;
  }

  public void setSelectQuery(SelectQuery selectQuery) {
    this.selectQuery = selectQuery;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
  
}
