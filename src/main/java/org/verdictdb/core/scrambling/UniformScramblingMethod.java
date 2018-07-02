package org.verdictdb.core.scrambling;

import java.util.Arrays;
import java.util.List;

public class UniformScramblingMethod implements ScramblingMethod {

  @Override
  public int getTierCount() {
    return 1;
  }

  @Override
  public List<String> getTierExpression(List<String> columnNames) {
    return Arrays.asList();
  }

  @Override
  public List<Double> getCumulativeBlockSizeDistribution(int tier) {
    // TODO Auto-generated method stub
    return null;
  }

}
