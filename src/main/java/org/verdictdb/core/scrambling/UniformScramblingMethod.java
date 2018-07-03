package org.verdictdb.core.scrambling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UniformScramblingMethod implements ScramblingMethod {

  @Override
  public int getTileCount() {
    return 1;
  }

  @Override
  public List<String> getTierExpression(List<String> columnNames) {
    return Arrays.asList();
  }

  @Override
  public List<Double> getCumulativeSizes(int tile, int length) {
    List<Double> cumul = new ArrayList<>();
    double c = 1.0 / length;
    for (int i = 0; i < length; i++) {
      cumul.add(c * (i+1));
    }
    return cumul;
  }

}
