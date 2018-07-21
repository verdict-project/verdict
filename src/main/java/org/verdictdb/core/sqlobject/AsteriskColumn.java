package org.verdictdb.core.sqlobject;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class AsteriskColumn implements UnnamedColumn, SelectItem {
  
  private static final long serialVersionUID = -930230895524125223L;
  
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
  public boolean isAggregateColumn() {
    return false;
  }

  @Override
  public AsteriskColumn deepcopy() {
    return new AsteriskColumn();
  }
  
  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
  
}
