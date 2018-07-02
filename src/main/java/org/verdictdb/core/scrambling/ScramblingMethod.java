package org.verdictdb.core.scrambling;

import java.util.List;

public interface ScramblingMethod {
  
  public int getTierCount();
  
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
   * @return
   */
  public List<Double> getCumulativeBlockSizeDistribution(int tier);

}
