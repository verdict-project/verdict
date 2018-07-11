package org.verdictdb.core.sqlobject;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class AsteriskColumn implements UnnamedColumn, SelectItem, Serializable {

  private static final long serialVersionUID = -7576542933729463258L;
  
  String tablename = null;

  public AsteriskColumn() {};
  
  public static AsteriskColumn create() {
    return new AsteriskColumn();
  }

  public AsteriskColumn(String tablename) {
    this.tablename = tablename;
  }

  public String getTablename() {
    return tablename;
  }

  public void setTablename(String tablename) {
    this.tablename = tablename;
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

  @Override
  public boolean isAggregateColumn() {
    return false;
  }

  @Override
  public AsteriskColumn deepcopy() {
    return new AsteriskColumn();
  }
  
}
