package org.verdictdb.core.scrambling;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ScramblingMethodBase implements ScramblingMethod {

  protected final long blockSize;
  
  private final Map<Integer, List<Double>> storedProbDist = new HashMap<>();
  
  public ScramblingMethodBase(long blockSize) {
    this.blockSize = blockSize;
  }
  
  long getBlockSize() {
    return blockSize;
  }
  
  protected void storeCumulativeProbabilityDistribution(int tier, List<Double> dist) {
    storedProbDist.put(tier, dist);
  }
  
  @Override
  public List<Double> getStoredCumulativeProbabilityDistributionForTier(int tier) {
    return storedProbDist.get(tier);
  }

}
