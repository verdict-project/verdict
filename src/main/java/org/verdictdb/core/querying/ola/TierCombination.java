package org.verdictdb.core.querying.ola;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

public class TierCombination implements Iterable<Map.Entry<Pair<String, String>, Integer>> {
  
  /**
   * Map of
   * 1. Pairs of a schema name and a table name
   * 2. Its tier value
   */
  private Map<Pair<String, String>, Integer> tableToTier = new HashMap<>();
  
  /**
   * The tier identifier (currently an integer) for an associated (schema, table). The length of
   * `originalTables` and the length of `values` must be equal.
   */
//  private List<Integer> values;
  
  public TierCombination(List<Pair<String, String>> originalTables, List<Integer> values) {
    for (int i = 0; i < originalTables.size(); i++) {
      tableToTier.put(originalTables.get(i), values.get(i));
    }
  }
  
  public int getTierNumberFor(String schemaName, String tableName) {
    return tableToTier.get(Pair.of(schemaName, tableName));
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TierCombination that = (TierCombination) o;
    return Objects.equals(tableToTier, that.tableToTier);
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(tableToTier);
  }
  
  @Override
  public String toString() {
    return "TierCombination{" +
               "tableToTier=" + tableToTier +
               '}';
  }
  
  @Override
  public Iterator<Entry<Pair<String, String>, Integer>> iterator() {
    return tableToTier.entrySet().iterator();
  }
}
