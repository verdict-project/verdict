package org.verdictdb.core.sqlobject;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class OrderbyAttribute {

  AliasReference aliasName;

  String order = "asc";

  public OrderbyAttribute(String attributeName) {
    this.aliasName = new AliasReference(attributeName);
  }

  public OrderbyAttribute(String attributeName, String order) {
    this.aliasName = new AliasReference(attributeName);
    this.order = order;
  }

  public String getAttributeName() {
    return aliasName.getAliasName();
  }

  public String getOrder() {
    return order;
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
