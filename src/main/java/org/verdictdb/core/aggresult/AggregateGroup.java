package org.verdictdb.core.aggresult;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class AggregateGroup {
  
  List<String> attributeNames;
  
  List<Object> attributeValues;
  
  public AggregateGroup(List<String> attributeNames, List<Object> attributeValues) {
    this.attributeNames = attributeNames;
    this.attributeValues = attributeValues;
  }
  
  public static AggregateGroup empty() {
    return new AggregateGroup(Arrays.<String>asList(), Arrays.asList());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((attributeNames == null) ? 0 : attributeNames.hashCode());
    result = prime * result + ((attributeValues == null) ? 0 : attributeValues.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    AggregateGroup other = (AggregateGroup) obj;
    if (attributeNames == null) {
      if (other.attributeNames != null)
        return false;
    } else if (!attributeNames.equals(other.attributeNames))
      return false;
    if (attributeValues == null) {
      if (other.attributeValues != null)
        return false;
    } else if (!attributeValues.equals(other.attributeValues))
      return false;
    return true;
  }

  @Override
  public String toString() {
      return ToStringBuilder.reflectionToString(this);
  }
}
