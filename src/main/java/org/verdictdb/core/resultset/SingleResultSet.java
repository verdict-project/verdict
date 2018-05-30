package org.verdictdb.core.resultset;

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
public class SingleResultSet {
  
  List<String> orderedColumnNames;
  
  Map<ResultSetGroup, ResultSetMeasures> data = new HashMap<>();
  
  public SingleResultSet(List<String> orderedColumnNames) throws ValueException {
    this.orderedColumnNames = orderedColumnNames;
    Set<String> colNames = new HashSet<>(orderedColumnNames);
    if (colNames.size() != orderedColumnNames.size()) {
      throw new ValueException("The column names seem to include duplicates.");
    }
  }
  
  public List<String> getColumnNames() {
    return orderedColumnNames;
  }
  
  public void addRow(ResultSetGroup group, ResultSetMeasures measures) {
    data.put(group, measures);
  }
  
  public void addRow(ResultSetMeasures measures) {
    data.put(ResultSetGroup.empty(), measures);
  }
  
  public ResultSetMeasures getMeasures(ResultSetGroup group) {
    return data.get(group);
  }
  
  public ResultSetMeasures getMeasures() throws ValueException {
    if (data.size() > 1) {
      throw new ValueException("The number of rows is larger than 1. A group must be specified.");
    }
    return data.get(ResultSetGroup.empty());
  }
  
  public Set<Entry<ResultSetGroup, ResultSetMeasures>> groupAndMeasuresSet() {
    return data.entrySet();
  }
}
