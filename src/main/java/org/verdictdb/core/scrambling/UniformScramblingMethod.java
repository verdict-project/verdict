package org.verdictdb.core.scrambling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.sqlobject.UnnamedColumn;

public class UniformScramblingMethod extends ScramblingMethodBase {
  
  public UniformScramblingMethod(long blockSize) {
    super(blockSize);
  }

  @Override
  public List<ExecutableNodeBase> getStatisticsNode(
      String oldSchemaName, String oldTableName, String columnMetaTokenKey, String partitionMetaTokenKey) {
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
  public List<UnnamedColumn> getTierExpressions(Map<String, Object> metaData) {
    return Arrays.asList();
  }

  @Override
  public List<Double> getCumulativeProbabilityDistributionForTier(
      Map<String, Object> metaData, 
      int tier) {
    
    DbmsQueryResult tableSizeResult = (DbmsQueryResult) metaData.get("0queryResult");
    tableSizeResult.next();
    long tableSize = tableSizeResult.getLong(0);
    long totalNumberOfblocks = (long) Math.ceil(tableSize / (float) blockSize);
    
    List<Double> prob = new ArrayList<>();
    for (int i = 0; i < totalNumberOfblocks; i++) {
      prob.add((i+1) / (double) totalNumberOfblocks);
    }
    
    return prob;
  }

//  @Override
//  public int getBlockCount(Map<String, Object> metaData) {
//    // TODO: infer this
//    return 100;
//  }

}
