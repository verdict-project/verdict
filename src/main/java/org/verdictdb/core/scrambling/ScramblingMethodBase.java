/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.core.scrambling;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = UniformScramblingMethod.class, name = "uniform"),
  @JsonSubTypes.Type(value = FastConvergeScramblingMethod.class, name = "fastconverge"),
  @JsonSubTypes.Type(value = HashScramblingMethod.class, name = "hash")
})
public abstract class ScramblingMethodBase implements ScramblingMethod, Serializable {

  private static final long serialVersionUID = 8767179573855372459L;

  protected long blockSize;

  protected String type = "base";

  protected final int maxBlockCount;

  protected final double relativeSize; // size relative to original table (0.0 ~ 1.0)

  protected Map<Integer, List<Double>> storedProbDist = new HashMap<>();

  public ScramblingMethodBase(long blockSize, int maxBlockCount, double relativeSize) {
    this.blockSize = blockSize;
    this.maxBlockCount = maxBlockCount;
    this.relativeSize = relativeSize;
  }

  public ScramblingMethodBase(Map<Integer, List<Double>> probDist) {
    this.storedProbDist = probDist;
    // uses the first tier prob. dist. to calculate the relative size for now.
    this.relativeSize = probDist.get(0).get(probDist.get(0).size() - 1);
    // dyoon: with stored prob. dist. these values are not necessary?
    this.blockSize = -1;
    this.maxBlockCount = 1;
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

  public int getMaxBlockCount() {
    return maxBlockCount;
  }

  @Override
  public double getRelativeSize() {
    return relativeSize;
  }
}
