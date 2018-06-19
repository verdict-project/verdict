package org.verdictdb.core.rewriter.aggresult;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class AggNameAndType {
  
  String aliasName;
  
  String aggType;
  
  public AggNameAndType(String aliasName, String aggType) {
    this.aliasName = aliasName.toLowerCase();
    this.aggType = aggType.toLowerCase();
  }

  public String getName() {
    return aliasName;
  }

  public String getAggType() {
    return aggType;
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
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
