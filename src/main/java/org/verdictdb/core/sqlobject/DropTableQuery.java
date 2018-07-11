package org.verdictdb.core.sqlobject;

public class DropTableQuery implements SqlConvertible {
  
  private static final long serialVersionUID = -3481351240470800158L;

  String schemaName;
  
  String tableName;

  public DropTableQuery(String schemaName, String tableName) {
    this.schemaName = schemaName;
    this.tableName = tableName;
  }
  
  public static DropTableQuery create(String schemaName, String tableName) {
    return new DropTableQuery(schemaName, tableName);
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }

}
