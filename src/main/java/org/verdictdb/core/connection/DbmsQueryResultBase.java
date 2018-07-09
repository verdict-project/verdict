package org.verdictdb.core.connection;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public abstract class DbmsQueryResultBase implements DbmsQueryResult {

  @Override
  public String getString(int index) {
    Object value = getValue(index);
    if (value == null) {
      return null;
    }
    return String.valueOf(value);
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
  public long getLong(int index) {
    Object value = getValue(index);
    if (value == null) {
      return 0;
    }
    return TypeCasting.toLong(value);
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
  public float getFloat(int index) {
    Object value = getValue(index);
    if (value == null) {
      return 0;
    }
    return TypeCasting.toFloat(value);
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

}
