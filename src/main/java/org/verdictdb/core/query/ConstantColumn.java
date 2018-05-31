package org.verdictdb.core.query;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class ConstantColumn implements UnnamedColumn, SelectItem {
    
    Object value;
    
    public void setValue(Object value) {
        this.value = value;
    }
    
    public Object getValue() {
        return value;
    }

    public static ConstantColumn valueOf(int value) {
        ConstantColumn c = new ConstantColumn();
        c.setValue(Integer.valueOf(value));
        return c;
    }
    
    public static ConstantColumn valueOf(double value) {
      ConstantColumn c = new ConstantColumn();
      c.setValue(Double.valueOf(value));
      return c;
  }
    
    public static ConstantColumn valueOf(String value) {
        ConstantColumn c = new ConstantColumn();
        c.setValue(value);
        return c;
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
