package org.verdictdb.commons;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.verdictdb.connection.TypeCasting;

public abstract class AttributeValueRetrievalHelper {
  
  private Map<String, Integer> lazyLabel2IndexMap = null;

  protected Integer getIndexOf(String label) {
    if (lazyLabel2IndexMap == null) {
      constructLabel2IndexMap();
    }
    return lazyLabel2IndexMap.get(label);
  }
  
  private void constructLabel2IndexMap() {
    int columnCount = getColumnCount();
    
    lazyLabel2IndexMap = new HashMap<>();
    for (int i = 0; i < columnCount; i++) {
      lazyLabel2IndexMap.put(getColumnName(i), i);
    }
  }
  
  public abstract String getColumnName(int i);

  public abstract int getColumnCount();

  public abstract Object getValue(int index);

  
  // Actual implementations provided by this class
  
  public String getString(int index) {
    Object value = getValue(index);
    if (value == null) {
      return null;
    }
    return String.valueOf(value);
  }
  
  public String getString(String label) {
    int index = getIndexOf(label);
    return getString(index);
  }

  public int getInt(int index) {
    Object value = getValue(index);
    if (value == null) {
      return 0;
    }
    return TypeCasting.toInteger(value);
  }
  
  public int getInt(String label) {
    int index = getIndexOf(label);
    return getInt(index);
  }

  public long getLong(int index) {
    Object value = getValue(index);
    if (value == null) {
      return 0;
    }
    return TypeCasting.toLong(value);
  }
  
  public long getLong(String label) {
    int index = getIndexOf(label);
    return getLong(index);
  }

  public double getDouble(int index) {
    Object value = getValue(index);
    if (value == null) {
      return 0;
    }
    return TypeCasting.toDouble(value);
  }
  
  public double getDouble(String label) {
    int index = getIndexOf(label);
    return getDouble(index);
  }

  public float getFloat(int index) {
    Object value = getValue(index);
    if (value == null) {
      return 0;
    }
    return TypeCasting.toFloat(value);
  }
  
  public float getFloat(String label) {
    int index = getIndexOf(label);
    return getFloat(index);
  }

  public Date getDate(int index) {
    Object value = getValue(index);
    
    if (value == null) {
      return null;
    }
    
    if (value instanceof Date){
      return (Date) value;
    }
    else if (value instanceof Timestamp) {
      return new Date(((Timestamp) value).getTime());
    }
    else if (value instanceof Time) {
      return new Date(((Time) value).getTime());
    }
    else {
      return null;
//      throw new VerdictDBTypeException("Could not obtain Date from: " + value);
    }
  }
  
  public Date getDate(String label) {
    int index = getIndexOf(label);
    return getDate(index);
  }
  

  public byte getByte(int index) {
    Object value = getValue(index);
    
    if (value == null) {
      return 0;
    }
    
    return (byte) TypeCasting.toByte(value);
  }

  public byte getByte(String label) {
    int index = getIndexOf(label);
    return getByte(index);
  }

  public Timestamp getTimestamp(int index) {
    Object value = getValue(index);
    
    if (value == null) {
      return null;
    }
    
    if (value instanceof Date){
      return new Timestamp(((Date) value).getTime());
    }
    else if (value instanceof Time) {
      return new Timestamp(((Time) value).getTime());
    }
    else if (value instanceof Timestamp) {
      return (Timestamp) value;
    }
    else {
      return null;
//      throw new VerdictDBTypeException("Could not obtain Timestamp from: " + value);
    }
  }
  
  public Timestamp getTimestamp(String label) {
    int index = getIndexOf(label);
    return getTimestamp(index);
  }

}
