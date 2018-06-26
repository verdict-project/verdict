package org.verdictdb.core.execution;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.verdictdb.core.aggresult.AggregateFrame;

public class ExecutionInfoToken {
  
  Map<String, Object> data = new HashMap<>();
  
  public static ExecutionInfoToken empty() {
    return new ExecutionInfoToken();
  }
  
  public Object getValue(String key) {
    return data.get(key);
  }
  
  public void setKeyValue(String key, Object value) {
    data.put(key, value);
  }
  
  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }

}
