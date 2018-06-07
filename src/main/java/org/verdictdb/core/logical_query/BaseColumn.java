package org.verdictdb.core.logical_query;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class BaseColumn implements UnnamedColumn, SelectItem, GroupingAttribute {

  String schemaName = "";

  String tableSourceAlias = "";

  String tableName = "";

  String columnName;

  public String getTableSourceAlias() {
    return tableSourceAlias;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() { return tableName; }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public void setTableSourceAlias(String tableSourceAlias) {
    this.tableSourceAlias = tableSourceAlias;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public void setTableName(String tableName) { this.tableName = tableName; }

  public BaseColumn(String columnName) {
    this.columnName = columnName;
  }

  public BaseColumn(String tableSourceAlias, String columnName) {
    this.tableSourceAlias = tableSourceAlias;
    this.columnName = columnName;
  }

  public BaseColumn(String schemaName, String tableSourceAlias, String columnName) {
    this.schemaName = schemaName;
    this.tableSourceAlias = tableSourceAlias;
    this.columnName = columnName;
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public boolean equals(Object obj) {
    return EqualsBuilder.reflectionEquals(this, obj);
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }
}
