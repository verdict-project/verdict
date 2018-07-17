package org.verdictdb.core.aggresult;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.verdictdb.commons.AttributeValueRetrievalHelper;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.DbmsQueryResultMetaData;

public class AggregateFrameQueryResult extends AttributeValueRetrievalHelper implements DbmsQueryResult {

  private AggregateFrame aggregateFrame;
  private transient Iterator it;
  private Map.Entry currentEntry;
  private List<Integer> orderedColumnIndex = new ArrayList<>();


  public AggregateFrameQueryResult(AggregateFrame aggregateFrame) {
    this.aggregateFrame = aggregateFrame;
    it = aggregateFrame.data.entrySet().iterator();
    List<String> orderedColumnName = aggregateFrame.getColumnNames();
    if (!aggregateFrame.data.entrySet().isEmpty()) {
      AggregateGroup group =((AggregateGroup)(aggregateFrame.data.keySet().toArray()[0]));
      AggregateMeasures measures = (AggregateMeasures)(aggregateFrame.data.values().toArray()[0]);
      for (int i=0; i<group.attributeNames.size();i++){
        orderedColumnIndex.add(orderedColumnName.indexOf(group.attributeNames.get(i)));
      }
      for (int i=0; i<measures.attributeNames.size();i++){
        orderedColumnIndex.add(orderedColumnName.indexOf(measures.attributeNames.get(i)));
      }
    }
  }

  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
    ois.defaultReadObject();
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
  public DbmsQueryResultMetaData getMetaData() {
    return aggregateFrame.dbmsQueryResultMetaData;
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
    index = orderedColumnIndex.get(index);
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

  @Override
  public void rewind() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public long getRowCount() {
    // TODO Auto-generated method stub
    return 0;
  }

}
