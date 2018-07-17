package org.verdictdb.core.querying;

import org.verdictdb.core.sqlobject.SqlConvertible;

public class CreateSchemaQuery implements SqlConvertible {
  
  private static final long serialVersionUID = -3471060043688549954L;

  String schemaName;
  
  boolean ifNotExists = false;
  
  public CreateSchemaQuery(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

}
