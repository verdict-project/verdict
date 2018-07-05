package org.verdictdb.core.scrambling;

import java.util.List;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.sqlobject.UnnamedColumn;

public class UniformScramblingMethod implements ScramblingMethod {

  @Override
  public StatiticsQueryGenerator getStatisticsQueryGenerator() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<UnnamedColumn> getTierExpressions(DbmsQueryResult statistics) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Double> getCumulativeProbabilityDistributionForTier(
      DbmsQueryResult statistics, 
      int tile, 
      int length) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getBlockCount(DbmsQueryResult statistics) {
    // TODO Auto-generated method stub
    return 0;
  }

}
