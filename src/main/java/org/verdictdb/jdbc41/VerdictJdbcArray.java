/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.jdbc41;

import static java.sql.Types.JAVA_OBJECT;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

public class VerdictJdbcArray implements Array {

  Object[] arr;

  public VerdictJdbcArray(Object[] arr) {
    this.arr = arr;
  }

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
    return arr == null ? null : Arrays.copyOfRange(arr, (int) index, (int) index + count);
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
  public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
      throws SQLException {
    throw new SQLException("Function is not supported yet");
  }

  @Override
  public void free() throws SQLException {}
}
