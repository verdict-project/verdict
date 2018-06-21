package org.verdictdb.core.query;

public class DropTableQuery {
  
  String schemaName;
  
  String tableName;

  public DropTableQuery(String schemaName, String tableName) {
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }

}
