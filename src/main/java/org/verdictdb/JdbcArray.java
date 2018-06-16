package org.verdictdb;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

import static java.sql.Types.JAVA_OBJECT;

public class JdbcArray implements Array {

  Object[] arr;

  public JdbcArray(Object[] arr) { this.arr = arr; }

  @Override
  public String getBaseTypeName() throws SQLException {
    return "JAVA_OBJECT";
  }

  @Override
  public int getBaseType() throws SQLException {
    return JAVA_OBJECT;
  }

  @Override
  public Object getArray() throws SQLException {
    return arr == null ? null : Arrays.copyOf(arr, arr.length);
  }

  @Override
  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    return getArray();
  }

  @Override
  public Object getArray(long index, int count) throws SQLException {
    return arr == null ? null : Arrays.copyOfRange(arr, (int)index, (int)index + count );
  }

  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    return getArray(index, count);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    throw new SQLException("Function is not supported yet");
  }

  @Override
  public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    throw new SQLException("Function is not supported yet");
  }

  @Override
  public ResultSet getResultSet(long index, int count) throws SQLException {
    throw new SQLException("Function is not supported yet");
  }

  @Override
  public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
    throw new SQLException("Function is not supported yet");
  }

  @Override
  public void free() throws SQLException { }
}
