package org.verdictdb.core.logical_query;


public class ConstantColumn implements AbstractColumn {
    
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
    
    public static ConstantColumn valueOf(String value) {
        ConstantColumn c = new ConstantColumn();
        c.setValue(value);
        return c;
    }

}
