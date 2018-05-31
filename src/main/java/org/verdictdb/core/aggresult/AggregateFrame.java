package org.verdictdb.core.aggresult;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.verdictdb.exception.ValueException;

/**
 * Represents a data frame (after aggregation).
 * 
 * @author Yongjoo Park
 *
 */
public class AggregateFrame {
  
  List<String> orderedColumnNames;
  
  Map<AggregateGroup, AggregateMeasures> data = new HashMap<>();
  
  public AggregateFrame(List<String> orderedColumnNames) throws ValueException {
    this.orderedColumnNames = orderedColumnNames;
    Set<String> colNames = new HashSet<>(orderedColumnNames);
    if (colNames.size() != orderedColumnNames.size()) {
      throw new ValueException("The column names seem to include duplicates.");
    }
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
