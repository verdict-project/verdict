package org.verdictdb.core.aggresult;

import org.verdictdb.connection.DbmsQueryResult;

import java.util.*;

public class AggregateFrameQueryResult implements DbmsQueryResult {

  private AggregateFrame aggregateFrame;
  private Iterator it;
  private Map.Entry currentEntry;

  public AggregateFrameQueryResult(AggregateFrame aggregateFrame) {
    this.aggregateFrame = aggregateFrame;
    it = aggregateFrame.data.entrySet().iterator();
  }

  public void setAggregateFrame(AggregateFrame aggregateFrame) {
    this.aggregateFrame = aggregateFrame;
    it = aggregateFrame.data.entrySet().iterator();
  }

  public AggregateFrame getAggregateFrame() {
    return aggregateFrame;
  }

  @Override
  public int getColumnCount(){
    return aggregateFrame.getColumnNames().size();
  }

  @Override
  public String getColumnName(int index){
    return aggregateFrame.getColumnNames().get(index);
  }

  @Override
  public int getColumnType(int index){
    return aggregateFrame.getColumnTypes().get(index);
  }

  @Override
  public boolean next() {
    if (it.hasNext()) {
      currentEntry = (Map.Entry)it.next();
      return true;
    }
    else return false;
  }

  @Override
  public Object getValue(int index) {
    // acquire the value in aggregateGroup
    index = aggregateFrame.getOrderedColumnIndex().get(index);
    if (index < ((AggregateGroup)currentEntry.getKey()).attributeValues.size()) {
      return ((AggregateGroup)currentEntry.getKey()).attributeValues.get(index);
    }
    else { // acquire the value in aggregateMeasure
      return ((AggregateMeasures)currentEntry.getValue()).attributeValues.get(index -
          ((AggregateGroup)currentEntry.getKey()).attributeValues.size());
    }
  }

  @Override
  public void printContent() {
    StringBuilder row;
    boolean isFirstCol = true;

    // print column names
    row = new StringBuilder();
    for (String col : aggregateFrame.getColumnNames()) {
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
