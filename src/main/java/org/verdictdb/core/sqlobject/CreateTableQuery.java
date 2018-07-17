package org.verdictdb.core.sqlobject;

public class CreateTableQuery implements SqlConvertible {

  private static final long serialVersionUID = 7842821425389712381L;
  
  protected String schemaName;

  protected String tableName;

  protected boolean ifNotExists = false;
  
  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }
  
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

}
