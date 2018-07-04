package org.verdictdb.core.scrambling;

import java.util.List;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.sqlobject.UnnamedColumn;

public interface ScramblingMethod {
  
  // Stage 1 is automatically run by ScramblingPlan
  
  // Stage 2 methods
  StatiticsQueryGenerator getStatisticsQueryGenerator();
  
  
  // Stage 3 methods
  public int getBlockCount(DbmsQueryResult statistics);
  
  /**
   * 
   * @param columnNames
   * @return A list of sql expressions (boolean predicates) that must be evaluated true at the step
   * indicating a particular tier 
   */
  public List<UnnamedColumn> getTierExpressions(DbmsQueryResult statistics);

  /**
   * 
   * @param tier 0, 1, ..., getTierCount()-1
   * @param length The length of the distribution
   * @return A list of doubles. The values should be increasing; the last value must be 1.0; and the size of 
   * the list must be equal to "length".
   */
  public List<Double> getCumulativeProbabilityDistributionForTier(
      DbmsQueryResult statistics, int tier, int length);

}
