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

package org.verdictdb.connection;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.verdictdb.commons.AttributeValueRetrievalHelper;

public class JdbcQueryResult extends AttributeValueRetrievalHelper implements DbmsQueryResult {

  private static final long serialVersionUID = 2576550992419489091L;

  List<String> columnNames = new ArrayList<>();

  List<Integer> columnTypes = new ArrayList<>();

  List<String> columnTypeNames = new ArrayList<>();

  //  ResultSet resultSet;

  List<List<Object>> result = new ArrayList<>();

  int cursor = -1;

  DbmsQueryResultMetaData dbmsQueryResultMetaData = new DbmsQueryResultMetaData();

  public JdbcQueryResult(ResultSet resultSet) throws SQLException {
    List<Boolean> isCurrency = new ArrayList<>();
    List<Integer> isNullable = new ArrayList<>();
    List<Integer> precision = new ArrayList<>();
    List<Integer> scale = new ArrayList<>();
    List<Integer> columnDisplaySize = new ArrayList<>();
    List<Boolean> isAutoIncrement = new ArrayList<>();
    List<String> columnClassName = new ArrayList<>();

    ResultSetMetaData meta = resultSet.getMetaData();
    int columnCount = meta.getColumnCount();
    for (int i = 0; i < columnCount; i++) {
      columnNames.add(meta.getColumnLabel(i + 1));
      columnTypes.add(meta.getColumnType(i + 1));
      columnTypeNames.add(meta.getColumnTypeName(i + 1));
      precision.add(meta.getPrecision(i + 1));
      scale.add(meta.getScale(i + 1));
      columnDisplaySize.add(meta.getColumnDisplaySize(i + 1));
      isNullable.add(meta.isNullable(i + 1));
      isCurrency.add(meta.isCurrency(i + 1));
      isAutoIncrement.add(meta.isAutoIncrement(i + 1));
      columnClassName.add(meta.getColumnClassName(i + 1));
    }
    dbmsQueryResultMetaData.columnDisplaySize = columnDisplaySize;
    dbmsQueryResultMetaData.isAutoIncrement = isAutoIncrement;
    dbmsQueryResultMetaData.isCurrency = isCurrency;
    dbmsQueryResultMetaData.isNullable = isNullable;
    dbmsQueryResultMetaData.precision = precision;
    dbmsQueryResultMetaData.scale = scale;
    dbmsQueryResultMetaData.columnClassName = columnClassName;

    while (resultSet.next()) {
      List<Object> row = new ArrayList<>();
      for (int i = 0; i < columnCount; i++) {
        //        if (resultSet.getMetaData().getColumnType(i+1) == BIT) {
        //          row.add(resultSet.getString(i+1));
        //        } else {
        Object value = resultSet.getObject(i + 1);
        row.add(value);
        //        }
      }
      result.add(row);
    }
  }

  @Override
  public DbmsQueryResultMetaData getMetaData() {
    return dbmsQueryResultMetaData;
  }

  @Override
  public int getColumnCount() {
    return columnNames.size();
  }

  @Override
  public String getColumnName(int index) {
    return columnNames.get(index);
  }

  @Override
  public int getColumnType(int index) {
    return columnTypes.get(index);
  }

  @Override
  public String getColumnTypeName(int index) {
    return columnTypeNames.get(index);
  }

  @Override
  public boolean next() {
    if (cursor < result.size() - 1) {
      cursor++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Object getValue(int index) {

    Object value = null;
    try {
      value = (Object) result.get(cursor).get(index);
      // value = resultSet.getObject(index + 1);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return value;
  }

  @Override
  public void printContent() {
    int oldCursor = cursor;
    rewind();

    StringBuilder row;
    boolean isFirstCol = true;

    // print column names
    row = new StringBuilder();
    for (String col : columnNames) {
      if (isFirstCol) {
        row.append(col);
        isFirstCol = false;
      } else {
        row.append("\t" + col);
      }
    }
    System.out.println(row.toString());

    // print contents
    int colCount = getColumnCount();
    while (next()) {
      row = new StringBuilder();
      for (int i = 0; i < colCount; i++) {
        if (i == 0) {
          row.append(getString(i));
        } else {
          row.append("\t");
          row.append(getString(i));
        }
      }
      System.out.println(row.toString());
    }

    // restore the cursor
    cursor = oldCursor;
  }

  public List<List<Object>> getResult() {
    return result;
  }

  @Override
  public void rewind() {
    cursor = -1;
  }

  @Override
  public long getRowCount() {
    return result.size();
  }
}
