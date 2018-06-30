package org.verdictdb.core.execution.ola;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Dimension {

  String schemaName;

  String tableName;

  int begin;

  int end;

  public Dimension(String schemaName, String tableName, int begin, int end) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.begin = begin;
    this.end = end;
  }

  public int length() {
    return end - begin + 1;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }

}
