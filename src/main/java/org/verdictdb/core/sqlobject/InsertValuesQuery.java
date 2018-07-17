package org.verdictdb.core.sqlobject;

import java.util.ArrayList;
import java.util.List;

public class InsertValuesQuery implements SqlConvertible {
  
  private static final long serialVersionUID = -1243370415019825285L;

  protected String schemaName;

  protected String tableName;
  
  protected List<Object> values = new ArrayList<>();
  
  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public List<Object> getValues() {
    return values;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }
  
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setValues(List<Object> values) {
    this.values = values;
  }

}
