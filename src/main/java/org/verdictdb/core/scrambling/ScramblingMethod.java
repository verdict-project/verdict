package org.verdictdb.core.scrambling;

import java.util.List;

public interface ScramblingMethod {
  
  public int getTileCount();
  
  /**
   * 
   * @param columnNames
   * @return A list of sql expressions (boolean predicates) that must be evaluated true at the step
   * indicating a particular tier 
   */
  public List<String> getTierExpression(List<String> columnNames);

  /**
   * 
   * @param tier 0, 1, ..., getTierCount()-1
   * @param length The length of the distribution
   * @return A list of doubles. The values should be increasing; the last value must be 1.0; and the size of 
   * the list must be length.
   */
  public List<Double> getCumulativeSizes(int tile, int length);

}
