package org.verdictdb.core.logical_query;

public class CreateTableAsSelect {

  String tableName;

  SelectQueryOp select;

  public CreateTableAsSelect(String tableName, SelectQueryOp select) {
    this.tableName = tableName;
    this.select = select;
  }

  public String getTableName() {
    return tableName;
  }

  public SelectQueryOp getSelect() {
    return select;
  }

}
