package org.verdictdb.core.connection;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

public class SparkQueryResult implements DbmsQueryResult {

  List<String> columnNames = new ArrayList<>();

  List<Integer> columnTypes = new ArrayList<>();

  List<List<Object>> result = new ArrayList<>();

  public SparkQueryResult(Dataset<Row> dataset) {
    scala.Tuple2<String,String>[] colNameAndColType = dataset.dtypes();
    for (scala.Tuple2<String,String> pair:colNameAndColType) {
      columnNames.add(pair._1);
      columnTypes.add(DataTypeConverter.stringToIntMap.get(pair._2));
    }
  }

  @Override
  public DbmsQueryResultMetaData getMetaData() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getColumnCount() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getColumnName(int index) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getColumnType(int index) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean next() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Object getValue(int index) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void printContent() {
    // TODO Auto-generated method stub

  }

}
