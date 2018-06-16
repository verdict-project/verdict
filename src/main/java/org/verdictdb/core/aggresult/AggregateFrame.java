package org.verdictdb.core.aggresult;

import java.lang.reflect.Array;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.exception.ValueException;

/**
 * Represents a data frame (after aggregation).
 * 
 * @author Yongjoo Park
 *
 */
public class AggregateFrame {
  
  List<String> orderedColumnNames;

  List<Integer> orderedColumnIndex = new ArrayList<>();

  List<Integer> columnTypes = new ArrayList<>();
  
  Map<AggregateGroup, AggregateMeasures> data = new HashMap<>();
  
  public AggregateFrame(List<String> orderedColumnNames) throws ValueException {
    this.orderedColumnNames = orderedColumnNames;
    Set<String> colNames = new HashSet<>(orderedColumnNames);
    if (colNames.size() != orderedColumnNames.size()) {
      throw new ValueException("The column names seem to include duplicates.");
    }
  }

  public static AggregateFrame fromDmbsQueryResult(DbmsQueryResult result, List<String> nonaggColumnsName, List<Pair<String, String>> aggColumns) throws ValueException {
    List<String> colName = new ArrayList<>();
    List<Integer> colIndex = new ArrayList<>();
    List<String> aggColumnsName = new ArrayList<>();

    for (Pair<String, String> pair:aggColumns) {
      aggColumnsName.add(pair.getKey());
    }
    HashSet<String> aggColumnsSet = new HashSet<>(aggColumnsName);
    HashSet<String> nonaggColumnsSet = new HashSet<>(nonaggColumnsName);
    List<Integer> aggColumnIndex = new ArrayList<>();
    List<Integer> nonaggColumnIndex = new ArrayList<>();
    List<String> orderedAggColumnName = new ArrayList<>();
    List<String> orderedNonaggColumnName = new ArrayList<>();
    List<Integer> columnTypes = new ArrayList<>();

    // Get Ordered column name for nonAgg and Agg
    for (int i=0;i<result.getColumnCount();i++) {
      colName.add(result.getColumnName(i));
      columnTypes.add(result.getColumnType(i));
      if (aggColumnsSet.contains(result.getColumnName(i))) {
        orderedAggColumnName.add(result.getColumnName(i));
        aggColumnIndex.add(i);
      }
      else if (nonaggColumnsSet.contains(result.getColumnName(i))) {
        orderedNonaggColumnName.add(result.getColumnName(i));
        nonaggColumnIndex.add(i);
      }
      else {
        throw new ValueException("The column belongs to nothing.");
      }
    }

    AggregateFrame aggregateFrame = new AggregateFrame(colName);
    aggregateFrame.setColumnTypes(columnTypes);

    // Set up the ordered column index
    for (String col : colName) {
      boolean find = false;
      for (int i=0;i<orderedNonaggColumnName.size();i++) {
        if (col.equals(orderedNonaggColumnName.get(i))) {
          colIndex.add(i);
          find = true;
          break;
        }
      }
      if (find) continue;
      for (int i=0;i<orderedAggColumnName.size();i++) {
        if (col.equals(orderedAggColumnName.get(i))) {
          colIndex.add(i+orderedNonaggColumnName.size());
          break;
        }
      }
    }
    aggregateFrame.setOrderedColumnIndex(colIndex);

    while (result.next()) {
      List<Object> aggValue = new ArrayList<>();
      List<Object> nonaggValue = new ArrayList<>();
      for (int i : aggColumnIndex) {
        aggValue.add(result.getValue(i));
      }
      for (int i : nonaggColumnIndex) {
        nonaggValue.add(result.getValue(i));
      }
      aggregateFrame.addRow(new AggregateGroup(orderedNonaggColumnName, nonaggValue), new AggregateMeasures(orderedAggColumnName, aggValue));
    }
    return aggregateFrame;
  }

  public DbmsQueryResult toDbmsQueryResult() {
    return new AggregateFrameQueryResult(this);
  }

  public void setColumnTypes(List<Integer> columnTypes) {
    this.columnTypes = columnTypes;
  }

  public void setOrderedColumnIndex(List<Integer> orderedColumnIndex) {
    this.orderedColumnIndex = orderedColumnIndex;
  }

  public List<Integer> getOrderedColumnIndex() {
    return orderedColumnIndex;
  }

  public List<Integer> getColumnTypes() {
    return columnTypes;
  }

  public List<String> getColumnNames() {
    return orderedColumnNames;
  }
  
  public void addRow(AggregateGroup group, AggregateMeasures measures) {
    data.put(group, measures);
  }
  
  public void addRow(AggregateMeasures measures) {
    data.put(AggregateGroup.empty(), measures);
  }
  
  public AggregateMeasures getMeasures(AggregateGroup group) {
    return data.get(group);
  }
  
  public AggregateMeasures getMeasures() throws ValueException {
    if (data.size() > 1) {
      throw new ValueException("The number of rows is larger than 1. A group must be specified.");
    }
    return data.get(AggregateGroup.empty());
  }
  
  public Set<Entry<AggregateGroup, AggregateMeasures>> groupAndMeasuresSet() {
    return data.entrySet();
  }
}
