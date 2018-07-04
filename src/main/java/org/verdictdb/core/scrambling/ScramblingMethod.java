package org.verdictdb.core.scrambling;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.sqlobject.SelectQuery;

public interface ScramblingMethod {
  
  // Stage 1 is automatically run by ScramblingPlan
  
  // Stage 2 methods
  SelectQuery getStatisticsQuery(List<Pair<String, Integer>> columnNamesAndTypes);
  
  // Stage 3 methods
  public int getTierCount(DbmsQueryResult statistics);
  
  /**
   * 
   * @param columnNames
   * @return A list of sql expressions (boolean predicates) that must be evaluated true at the step
   * indicating a particular tier 
   */
  public List<String> getTierExpressions(DbmsQueryResult statistics);

  /**
   * 
   * @param tier 0, 1, ..., getTierCount()-1
   * @param length The length of the distribution
   * @return A list of doubles. The values should be increasing; the last value must be 1.0; and the size of 
   * the list must be length.
   */
  public List<Double> getCumulativeProbabilityDistributionForTier(
      DbmsQueryResult statistics, 
      int tile, 
      int length);

}
