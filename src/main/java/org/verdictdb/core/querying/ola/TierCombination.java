package org.verdictdb.core.querying.ola;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class TierCombination implements Iterable<Map.Entry<Pair<String, String>, Integer>>, Comparable<TierCombination>  {
  
  /**
   * Map of
   * 1. Pairs of a schema name and a table name
   * 2. Its tier value
   */
  private Map<Pair<String, String>, Integer> tableToTier = new TreeMap<>();
  
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


  @Override
  public int compareTo(TierCombination o) {
    Object[] keysets1 = o.tableToTier.keySet().toArray();
    Object[] keysets2 = this.tableToTier.keySet().toArray();
    for (int i=0;i<keysets2.length;i++) {
      Pair<String, String> x, y;
      if (i>=keysets1.length) {
        return 1;
      }
      x = (ImmutablePair)keysets1[i];
      y = (ImmutablePair)keysets2[i];
      if (y.compareTo(x)!=0) {
        return y.compareTo(x);
      }
      Integer xx = (Integer) o.tableToTier.values().toArray()[i];
      Integer yy = (Integer) this.tableToTier.values().toArray()[i];
      if (yy.compareTo(xx)!=0) {
        return yy.compareTo(xx);
      }
    }
    if (keysets1.length==keysets2.length) {
      return 0;
    }
    else return -1;
  }
}
