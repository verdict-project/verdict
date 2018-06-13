package org.verdictdb;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TypeCasting {

  public static Double toDouble(Object obj) throws SQLException {
    if (obj instanceof Double)
      return (Double) obj;
    else if (obj instanceof Float)
      return ((Float) obj).doubleValue();
    else if (obj instanceof BigDecimal)
      return ((BigDecimal) obj).doubleValue();
    else if (obj instanceof Long)
      return ((Long) obj).doubleValue();
    else if (obj instanceof Integer)
      return ((Integer) obj).doubleValue();
    else if (obj instanceof Short)
      return ((Short) obj).doubleValue();
    else if (obj instanceof Byte)
      return ((Byte) obj).doubleValue();
    else {
     throw new SQLException("Not supported data type.");
    }
  }

  public static Float toFloat(Object obj) throws SQLException {
    if (obj instanceof Double)
      return ((Double) obj).floatValue();
    else if (obj instanceof Float)
      return ((Float) obj);
    else if (obj instanceof BigDecimal)
      return ((BigDecimal) obj).floatValue();
    else if (obj instanceof Long)
      return ((Long) obj).floatValue();
    else if (obj instanceof Integer)
      return ((Integer) obj).floatValue();
    else if (obj instanceof Short)
      return ((Short) obj).floatValue();
    else if (obj instanceof Byte)
      return ((Byte) obj).floatValue();
    else {
      throw new SQLException("Not supported data type.");
    }
  }

  public static BigDecimal toBigDecimal(Object obj) throws SQLException {
    if (obj instanceof Double)
      return new BigDecimal((Double) obj);
    else if (obj instanceof Float)
      return new BigDecimal((Float) obj);
    else if (obj instanceof BigDecimal)
      return ((BigDecimal) obj);
    else if (obj instanceof Long)
      return new BigDecimal((Long) obj);
    else if (obj instanceof Integer)
      return new BigDecimal((Integer) obj);
    else if (obj instanceof Short)
      return new BigDecimal((Short) obj);
    else if (obj instanceof Byte)
      return new BigDecimal((Byte) obj);
    else {
      throw new SQLException("Not supported data type.");
    }
  }

  public static BigDecimal toBigDecimal(Object obj, int scale) throws SQLException {
    if (obj instanceof Double)
      return new BigDecimal((Double) obj).setScale(scale);
    else if (obj instanceof Float)
      return new BigDecimal((Float) obj).setScale(scale);
    else if (obj instanceof BigDecimal)
      return ((BigDecimal) obj).setScale(scale);
    else if (obj instanceof Long)
      return new BigDecimal((Long) obj).setScale(scale);
    else if (obj instanceof Integer)
      return new BigDecimal((Integer) obj).setScale(scale);
    else if (obj instanceof Short)
      return new BigDecimal((Short) obj).setScale(scale);
    else if (obj instanceof Byte)
      return new BigDecimal((Byte) obj).setScale(scale);
    else {
      throw new SQLException("Not supported data type.");
    }
  }


  public static long toLong(Object obj) throws SQLException {
    if (obj instanceof Double)
      return ((Double) obj).intValue();
    else if (obj instanceof Float)
      return ((Float) obj).intValue();
    else if (obj instanceof BigDecimal)
      return ((BigDecimal) obj).toBigInteger().longValue();
    else if (obj instanceof Long)
      return ((Long) obj);
    else if (obj instanceof Integer)
      return ((Integer) obj);
    else if (obj instanceof Short)
      return ((Short) obj);
    else if (obj instanceof Byte)
      return ((Byte) obj);
    else {
      throw new SQLException("Not supported data type.");
    }
  }

  public static Integer toInteger(Object obj) throws SQLException {
    if (obj instanceof Double)
      return ((Double) obj).intValue();
    else if (obj instanceof Float)
      return ((Float) obj).intValue();
    else if (obj instanceof BigDecimal)
      return ((BigDecimal) obj).toBigInteger().intValue();
    else if (obj instanceof Long)
      return ((Long) obj).intValue();
    else if (obj instanceof Integer)
      return ((Integer) obj);
    else if (obj instanceof Short)
      return ((Short)obj).intValue();
    else if (obj instanceof Byte)
      return ((Byte) obj).intValue();
    else {
      throw new SQLException("Not supported data type.");
    }
  }

  public static Short toShort(Object obj) throws SQLException {
    if (obj instanceof Double)
      return ((Double) obj).shortValue();
    else if (obj instanceof Float)
      return ((Float) obj).shortValue();
    else if (obj instanceof BigDecimal)
      return ((BigDecimal) obj).toBigInteger().shortValue();
    else if (obj instanceof Long)
      return ((Long) obj).shortValue();
    else if (obj instanceof Integer)
      return ((Integer) obj).shortValue();
    else if (obj instanceof Short)
      return ((Short)obj);
    else if (obj instanceof Byte)
      return ((Byte) obj).shortValue();
    else {
      throw new SQLException("Not supported data type.");
    }
  }

  public static Byte toByte(Object obj) throws SQLException {
    if (obj instanceof Double)
      return ((Double) obj).byteValue();
    else if (obj instanceof Float)
      return ((Float) obj).byteValue();
    else if (obj instanceof BigDecimal)
      return ((BigDecimal) obj).toBigInteger().byteValue();
    else if (obj instanceof Long)
      return ((Long) obj).byteValue();
    else if (obj instanceof Integer)
      return ((Integer) obj).byteValue();
    else if (obj instanceof Short)
      return ((Short)obj).byteValue();
    else if (obj instanceof Byte)
      return ((Byte) obj);
    else {
      throw new SQLException("Not supported data type.");
    }
  }

}
