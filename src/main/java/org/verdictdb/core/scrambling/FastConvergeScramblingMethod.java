package org.verdictdb.core.scrambling;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.UnnamedColumn;

import com.google.common.base.Optional;

/**
 * 
 * Policy:
 * 1. Tier 0: tuples containing outlier values.
 * 2. Tier 1: tuples containing rare groups
 * 3. Tier 2: other tuples
 * 
 * Outlier values:
 * Bottom 0.1% and 99.9% percentile values for every numeric column. Suppose a table has 20 numeric columns; 
 * then, in the worst case, 20 * 0.2% = 4% of the tuples are the tuples containing outlier values.
 * 
 * Blocks for Tier 0:
 * Tier 0 takes up to 50% of each block.
 * 
 * Blocks for Tier 1:
 * Tier 0 + Tier 1 take up to 80% of each block.
 * 
 * Blocks for Tier 2:
 * Tier 2 takes the rest of the space in each block.
 * 
 * @author Yongjoo Park
 *
 */
public class FastConvergeScramblingMethod extends ScramblingMethodBase {

  final double p0 = 0.5;     // max portion for Tier 0; should be configured dynamically in the future
  
  final double p1 = 0.8;     // max portion for Tier 0 + Tier 1; should be configured dynamically in the future
  
  Optional<String> primaryColumnName = Optional.absent();
  
  public FastConvergeScramblingMethod(long blockSize) {
    super(blockSize);
  }
  
  public FastConvergeScramblingMethod(long blockSize, String primaryColumnName) {
    this(blockSize);
    this.primaryColumnName = Optional.of(primaryColumnName);
  }
  
  /**
   * Computes three nodes. They compute: (1) 0.1% and 99.9% percentiles of numeric columns and the total count,
   * (2) the sizes of "large" groups, and (3) 
   */
  @Override
  public List<ExecutableNodeBase> getStatisticsNode(String oldSchemaName, String oldTableName) {
    return Arrays.asList();
  }

  @Override
  public int getBlockCount(DbmsQueryResult statistics) {
    // TODO: will use the provided statistics
    return 10;
  }

  @Override
  public List<UnnamedColumn> getTierExpressions(DbmsQueryResult statistics) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Double> getCumulativeProbabilityDistributionForTier(
      DbmsQueryResult statistics, 
      int tier, 
      int length) {
    // TODO Auto-generated method stub
    return null;
  }

}


/**
 * Retrieves the following information.
 * 
 * 1. Top and bottom 0.1% percentile values of every numeric column.
 * 2. Total count
 * 
 * @author Yongjoo Park
 *
 */
class FastConvergeStatisticsQueryGenerator implements StatiticsQueryGenerator {

  @Override
  public SelectQuery create(
      String schemaName, 
      String tableName, 
      List<Pair<String, String>> columnNamesAndTypes, 
      List<String> partitionColumnNames) {
    
    
    // TODO Auto-generated method stub
    return null;
  }
  
}
