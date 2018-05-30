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
  
  public ResultSetMeasures getMeasures(ResultSetGroup group) {
    return data.get(group);
  }
  
  public Set<Entry<ResultSetGroup, ResultSetMeasures>> groupAndMeasuresSet() {
    return data.entrySet();
  }
}
