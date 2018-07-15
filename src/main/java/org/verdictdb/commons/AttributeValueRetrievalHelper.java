package org.verdictdb.commons;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import org.verdictdb.connection.TypeCasting;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.jdbc41.VerdictJdbcArray;

import javax.sql.rowset.serial.SerialBlob;

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

  public Boolean getBoolean(int index) throws SQLException {
    Object value = getValue(index);
    if (value == null) {
      return false;
    }
    if (value instanceof Boolean) {
      return (boolean) value;
    }
    if (value instanceof Integer || value instanceof Long || value instanceof Double || value instanceof Float) {
      int v = Integer.valueOf(value.toString());
      if (v == 1) {
        return true;
      } else if (v == 0) {
        return false;
      }
    } else if (value instanceof String) {
      String v = value.toString();
      if (v.equals("1")||v.equals("t")) {
        return true;
      } else if (v.equals("0")||v.equals("f")) {
        return false;
      }
    }
    throw new SQLException("Not a valid value for Boolean type: " + value);
  }

  public Boolean getBoolean(String label) throws SQLException {
    int index = getIndexOf(label);
    return getBoolean(index);
  }

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
    if (value instanceof String) {
      String v = value.toString();
      if (v.equals("1")||v.equals("t")) {
        return 1;
      } else if (v.equals("0")||v.equals("f")) {
        return 0;
      }
    }
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
    if (value instanceof String) {
      String v = value.toString();
      if (v.equals("1")||v.equals("t")) {
        return 1;
      } else if (v.equals("0")||v.equals("f")) {
        return 0;
      }
    }
    if (value == null) {
      return 0;
    }
    return TypeCasting.toLong(value);
  }
  
  public long getLong(String label){
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
    if (value instanceof String) {
      String v = value.toString();
      if (value.equals("1")||value.equals("t")) {
        return 1;
      } else if (value.equals("0")||value.equals("f")) {
        return 0;
      }
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
    }
  }
  
  public Timestamp getTimestamp(String label) {
    int index = getIndexOf(label);
    return getTimestamp(index);
  }

  public short getShort(int index) {
    Object value = getValue(index);
    if (value instanceof String) {
      String v = value.toString();
      if (v.equals("1")||v.equals("t")) {
        return 1;
      } else if (v.equals("0")||v.equals("f")) {
        return 0;
      }
    }
    if (value == null) {
      return 0;
    }
    return TypeCasting.toShort(value);
  }

  public short getShort(String label) {
    int index = getIndexOf(label);
    return getShort(index);
  }

  public BigDecimal getBigDecimal(int index, int scale) {
    Object value = TypeCasting.toBigDecimal(getValue(index), scale);
    return (BigDecimal) value;
  }

  public BigDecimal getBigDecimal(String label, int scale) {
    int index = getIndexOf(label);
    return getBigDecimal(index, scale);
  }

  public BigDecimal getBigDecimal(int index) {
    Object value = TypeCasting.toBigDecimal(getValue(index));
    return (BigDecimal) value;
  }

  public BigDecimal getBigDecimal(String label) {
    int index = getIndexOf(label);
    return getBigDecimal(index);
  }

  public byte[] getBytes(int index) {
    Object value = getValue(index);
    return (byte[]) value;
  }

  public byte[] getBytes(String label) {
    int index = getIndexOf(label);
    return getBytes(index);
  }

  public Time getTime(int index) {
    Object value = getValue(index);
    if (value == null) {
      return null;
    }
    if (value instanceof Date){
      return new Time(((Date) value).getTime());
    }
    else if (value instanceof Time) {
      return (Time) value;
    }
    else if (value instanceof Timestamp) {
      return new Time(((Timestamp) value).getTime());
    }
    else {
      return null;
    }
  }

  public Time getTime(String label) {
    int index = getIndexOf(label);
    return getTime(index);
  }

  public InputStream getAsciiStream(int index) {
    Object value = getValue(index);
    return (InputStream) value;
  }

  public InputStream getAsciiStream(String label) {
    int index = getIndexOf(label);
    return getAsciiStream(index);
  }

  public InputStream getUnicodeStream(int index) {
    Object value = getValue(index);
    return (InputStream) value;
  }

  public InputStream getUnicodeStream(String label) {
    int index = getIndexOf(label);
    return getUnicodeStream(index);
  }

  public InputStream getBinaryStream(int index) {
    Object value = getValue(index);

    if (value == null) {
      byte[] a = {};
      return new ByteArrayInputStream(a);
    }
    if (value instanceof byte[]){
      ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream((byte[]) value);
      return byteArrayInputStream;
    }
    return (InputStream) value;
  }

  public InputStream getBinaryStream(String label) {
    int index = getIndexOf(label);
    return getBinaryStream(index);
  }

  public Ref getRef(int index) {
    Object value = getValue(index);
    return (Ref) value;
  }

  public Ref getRef(String label) {
    int index = getIndexOf(label);
    return getRef(index);
  }

  public Blob getBlob(int index) throws SQLException {
    Object value = getValue(index);

    if (value == null) {
      return null;
    }
    if (value instanceof Blob) {
      return (Blob) value;
    }

    return new SerialBlob((byte[]) value);
  }

  public Blob getBlob(String label) throws SQLException {
    int index = getIndexOf(label);
    return getBlob(index);
  }

  public Clob getClob(int index) {
    Object value = getValue(index);
    return (Clob) value;
  }

  public Clob getClob(String label) {
    int index = getIndexOf(label);
    return getClob(index);
  }

  public Array getArray(int index) {
    Object value = getValue(index);
    VerdictJdbcArray array = new VerdictJdbcArray((Object[]) value);
    return array;
  }

  public Array getArray(String label) {
    int index = getIndexOf(label);
    return getArray(index);
  }

  public URL getURL(int index) {
    Object value = getValue(index);
    return (URL) value;
  }

  public URL getURL(String label) {
    int index = getIndexOf(label);
    return getURL(index);
  }

  public RowId getRowId(int index) {
    Object value = getValue(index);
    return (RowId) value;
  }

  public RowId getRowId(String label) {
    int index = getIndexOf(label);
    return getRowId(index);
  }

  public NClob getNClob(int index) {
    Object value = getValue(index);
    return (NClob) value;
  }

  public NClob getNClob(String label) {
    int index = getIndexOf(label);
    return getNClob(index);
  }

  public SQLXML getSQLXML(int index) {
    Object value = getValue(index);
    return (SQLXML) value;
  }

  public SQLXML getSQLXML(String label) {
    int index = getIndexOf(label);
    return getSQLXML(index);
  }

}
