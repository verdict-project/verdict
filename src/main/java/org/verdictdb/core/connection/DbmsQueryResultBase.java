package org.verdictdb.core.connection;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public abstract class DbmsQueryResultBase implements DbmsQueryResult {
  
  private static final long serialVersionUID = 6800748700764224903L;
  
  private Map<String, Integer> lazyLabel2IndexMap = null;
  
  protected Integer getIndexOf(String label) {
    if (lazyLabel2IndexMap == null) {
      constructLabel2IndexMap();
    }
    return lazyLabel2IndexMap.get(label);
  }
  
  private void constructLabel2IndexMap() {
    lazyLabel2IndexMap = new HashMap<>();
    for (int i = 0; i < getColumnCount(); i++) {
      lazyLabel2IndexMap.put(getColumnName(i), i);
    }
  }

  @Override
  public String getString(int index) {
    Object value = getValue(index);
    if (value == null) {
      return null;
    }
    return String.valueOf(value);
  }
  
  @Override
  public String getString(String label) {
    int index = getIndexOf(label);
    return getString(index);
  }

  @Override
  public int getInt(int index) {
    Object value = getValue(index);
    if (value == null) {
      return 0;
    }
    return TypeCasting.toInteger(value);
  }
  
  @Override
  public int getInt(String label) {
    int index = getIndexOf(label);
    return getInt(index);
  }

  @Override
  public long getLong(int index) {
    Object value = getValue(index);
    if (value == null) {
      return 0;
    }
    return TypeCasting.toLong(value);
  }
  
  @Override
  public long getLong(String label) {
    int index = getIndexOf(label);
    return getLong(index);
  }

  @Override
  public double getDouble(int index) {
    Object value = getValue(index);
    if (value == null) {
      return 0;
    }
    return TypeCasting.toDouble(value);
  }
  
  @Override
  public double getDouble(String label) {
    int index = getIndexOf(label);
    return getDouble(index);
  }

  @Override
  public float getFloat(int index) {
    Object value = getValue(index);
    if (value == null) {
      return 0;
    }
    return TypeCasting.toFloat(value);
  }
  
  @Override
  public float getFloat(String label) {
    int index = getIndexOf(label);
    return getFloat(index);
  }

  @Override
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
  
  @Override
  public Date getDate(String label) {
    int index = getIndexOf(label);
    return getDate(index);
  }

  @Override
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
  
  @Override
  public Timestamp getTimestamp(String label) {
    int index = getIndexOf(label);
    return getTimestamp(index);
  }

}
