package org.verdictdb.core.scrambling;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.sqlobject.SelectQuery;

public class UniformScramblingMethod implements ScramblingMethod {

  @Override
  public SelectQuery getStatisticsQuery(List<Pair<String, Integer>> columnNamesAndTypes) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getTierCount(DbmsQueryResult statistics) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public List<String> getTierExpressions(DbmsQueryResult statistics) {
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

}
