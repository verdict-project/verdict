package org.verdictdb.core.scrambling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.sqlobject.UnnamedColumn;

public class UniformScramblingMethod extends ScramblingMethodBase {
  
  public UniformScramblingMethod(long blockSize) {
    super(blockSize);
  }

  @Override
  public List<ExecutableNodeBase> getStatisticsNode(String oldSchemaName, String oldTableName) {
    return Arrays.asList();
  }

//  @Override
//  public StatiticsQueryGenerator getStatisticsQueryGenerator() {
//    return new StatiticsQueryGenerator() {
//      @Override
//      public SelectQuery create(
//          String schemaName, 
//          String tableName, 
//          List<Pair<String, String>> columnNamesAndTypes, List<String> partitionColumnNames) {
//        // TODO Auto-generated method stub
//        return null;
//      }
//    };
//  }

  @Override
  public List<UnnamedColumn> getTierExpressions(DbmsQueryResult statistics) {
    return Arrays.asList();
  }

  @Override
  public List<Double> getCumulativeProbabilityDistributionForTier(
      DbmsQueryResult statistics, 
      int tier, 
      int length) {
    
    List<Double> prob = new ArrayList<>();
    for (int i = 0; i < length; i++) {
      prob.add((i+1) / (double) length);
    }
    
    return prob;
  }

  @Override
  public int getBlockCount(DbmsQueryResult statistics) {
    // TODO: infer this
    return 100;
  }

}
