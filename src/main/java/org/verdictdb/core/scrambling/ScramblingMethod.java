package org.verdictdb.core.scrambling;

import java.util.List;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.sqlobject.SelectQuery;

public interface ScramblingMethod {
  
  SelectQuery getStatisticsQuery();
  
  public int getTileCount(DbmsQueryResult statistics);
  
  /**
   * 
   * @param columnNames
   * @return A list of sql expressions (boolean predicates) that must be evaluated true at the step
   * indicating a particular tier 
   */
  public List<String> getTileExpressions(DbmsQueryResult statistics);

  /**
   * 
   * @param tier 0, 1, ..., getTierCount()-1
   * @param length The length of the distribution
   * @return A list of doubles. The values should be increasing; the last value must be 1.0; and the size of 
   * the list must be length.
   */
  public List<Double> getCumulativeProbabilityDistributionForTile(
      DbmsQueryResult statistics, 
      int tile, 
      int length);

}
