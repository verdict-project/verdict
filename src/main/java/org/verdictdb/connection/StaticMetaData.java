package org.verdictdb.connection;

import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.tuple.ImmutablePair;

public class StaticMetaData {

  public enum dataType{
    type_long
  }

  public static class TableInfo {
    String schema;
    String tablename;

    public TableInfo(String schema, String tablename) {
      this.schema = schema;
      this.tablename = tablename;
    }

    public static TableInfo getTableInfo(String schema, String tablename) {
      return new TableInfo(schema, tablename);
    }

    @Override
    public boolean equals(Object obj) {
      return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
      return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString() {
      return ToStringBuilder.reflectionToString(this);
    }

  }

  private String defaultSchema = "";

  //The value pair: left is column name and right is its type
  private HashMap<TableInfo, List<ImmutablePair<String, dataType>>> tablesData = new HashMap<>();

  public StaticMetaData() {}

  public StaticMetaData(HashMap<TableInfo, List<ImmutablePair<String, dataType>>> tablesData) {
    this.tablesData = tablesData;
  }

  public void addTableData(TableInfo table, List<ImmutablePair<String, dataType>> columns) { tablesData.put(table, columns); }

  public void setDefaultSchema(String schema) { defaultSchema = schema; }

  public String getDefaultSchema() { return defaultSchema;}

  public HashMap<TableInfo, List<ImmutablePair<String, dataType>>> getTablesData() { return tablesData; }

}
