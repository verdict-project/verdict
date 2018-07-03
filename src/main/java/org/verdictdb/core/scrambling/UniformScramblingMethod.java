package org.verdictdb.core.scrambling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.sqlobject.SelectQuery;

public class UniformScramblingMethod implements ScramblingMethod {

  @Override
  public SelectQuery getStatisticsQuery() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getTileCount(DbmsQueryResult statistics) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public List<String> getTileExpressions(DbmsQueryResult statistics) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Double> getCumulativeProbabilityDistributionForTile(
      DbmsQueryResult statistics, 
      int tile, 
      int length) {
    // TODO Auto-generated method stub
    return null;
  }

}
