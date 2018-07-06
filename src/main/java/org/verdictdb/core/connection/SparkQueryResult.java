package org.verdictdb.core.connection;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

public class SparkQueryResult implements DbmsQueryResult {

  List<String> columnNames = new ArrayList<>();

  List<Integer> columnTypes = new ArrayList<>();

  List<Row> result = new ArrayList<>();

  int cursor = -1;

  public SparkQueryResult(Dataset<Row> dataset) {
    scala.Tuple2<String,String>[] colNameAndColType = dataset.dtypes();
    for (scala.Tuple2<String,String> pair:colNameAndColType) {
      columnNames.add(pair._1);
      columnTypes.add(SparkDataTypeConverter.typeInt(pair._2));
    }
    result = dataset.collectAsList();
  }

  @Override
  public DbmsQueryResultMetaData getMetaData() {
    // TODO Auto-generated method stub
    return null;
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
  public boolean next() {
    if (cursor < result.size()-1) {
      cursor++;
      return true;
    }
    else {
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
    StringBuilder row;
    boolean isFirstCol = true;

    // print column names
    row = new StringBuilder();
    for (String col : columnNames) {
      if (isFirstCol) {
        row.append(col);
        isFirstCol = false;
      }
      else {
        row.append("\t" + col);
      }
    }
    System.out.println(row.toString());

    // print contents
    int colCount = getColumnCount();
    while(this.next()) {
      row = new StringBuilder();
      for (int i = 0; i < colCount; i++) {
        if (i == 0) {
          row.append(getValue(i).toString());
        }
        else {
          row.append("\t");
          row.append(getValue(i).toString());
        }
      }
      System.out.println(row.toString());
    }
  }

}
