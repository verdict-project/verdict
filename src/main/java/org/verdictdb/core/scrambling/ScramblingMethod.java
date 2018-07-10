package org.verdictdb.core.scrambling;

import java.util.List;
import java.util.Map;

import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.UnnamedColumn;

public interface ScramblingMethod {
  
  // Stage 1 is automatically run by ScramblingPlan
  
  // Stage 2 methods
  List<ExecutableNodeBase> getStatisticsNode(
      String oldSchemaName, String oldTableName, String columnMetaTokenKey, String partitionMetaTokenKey);
//  StatiticsQueryGenerator getStatisticsQueryGenerator();
  
  
  // Stage 3 methods
//  public int getBlockCount(Map<String, Object> metaData);
  
  /**
   * 
   * @param columnNames
   * @return A list of sql expressions (boolean predicates) that must be evaluated true at the step
   * indicating a particular tier 
   */
  public List<UnnamedColumn> getTierExpressions(Map<String, Object> metaData);

  /**
   * 
   * @param tier 0, 1, ..., getTierCount()-1
   * @param length The length of the distribution
   * @return A list of doubles. The values should be increasing; the last value must be 1.0; and the size of 
   * the list must be equal to "length".
   */
  public List<Double> getCumulativeProbabilityDistributionForTier(
      Map<String, Object> metaData, int tier);


  /**
   * Returns the table that should be used in the final scrambling stage. This can be a join of the main table
   * and some auxiliary tables.
   * @param metaData 
   * 
   * @return
   */
  public AbstractRelation getScramblingSource(String originalSchema, String originalTable, Map<String, Object> metaData);
  
  public String getMainTableAlias();

}
