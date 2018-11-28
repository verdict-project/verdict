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

package org.verdictdb.core.querying.ola;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.UnnamedColumn;

import com.google.common.collect.Sets;
import com.rits.cloning.Cloner;

/**
 * Stores 
 * 1. hypercubes (that indicate aggregation blocks), and
 * 2. aggregate column alias names.
 */
public class AggMeta implements Serializable {
  
  private static final long serialVersionUID = 3186577687141707687L;
  
  /**
   * List of covered blocks
   */
  private List<HyperTableCube> cubes = new ArrayList<>();
  
  private List<SelectItem> originalSelectList;
  
  // later, these columns are summed for combining answers from smaller queries
  private List<String> aggAlias = new ArrayList<>();
  
  // later, these columns are either maxed or mined for combining answers for smaller queries.
  private Map<String, String> maxminAggAlias = new HashMap<>();
  
  /**
   * Mapping from scrambled table to the column alias name for its tier column in the associated
   * current select query.
   */
  private Map<ScrambleMeta, String> tierColumnForScramble = new HashMap<>();
  
  Map<SelectItem, List<ColumnOp>> aggColumn = new HashMap<>();
  
  // (agg function, argument), alias
  Map<Pair<String, UnnamedColumn>, String> aggColumnAggAliasPair = new HashMap<>();
  
  Map<Pair<String, UnnamedColumn>, String> aggColumnAggAliasPairOfMaxMin = new HashMap<>();
  
  String tierColumnName;
  
  public AggMeta() { }
  
  public Set<String> getAllTierColumnAliases() {
    return new HashSet<String>(tierColumnForScramble.values());
  }
  
  /**
   * Computes the scale factor for every combination of the tiers of involved scrambled tables.
   * The return value contains the scaling factor in terms of the original scrambled tables. The
   * tier column related to those original scrambled tables must be returned from another field
   * `tierColumnForScramble`
   *
   * @return Tier combination -> scaling factor for that tier.
   */
  public Map<TierCombination, Double> computeScaleFactors() {
    Map<TierCombination, Double> tierToScalingFactor = new TreeMap<>();
    ScrambleMetaSet metaset =
        ScrambleMetaSet.createFromCollection(tierColumnForScramble.keySet());
    
    List<TierCombination> tierCombinations = generateAllTierCombinations(metaset);
    for (TierCombination c : tierCombinations) {
      double ratio = ratioOfAllCubes(c, metaset);
      double scalingFactor = 1.0 / ratio;
      tierToScalingFactor.put(c, scalingFactor);
    }
    
    return tierToScalingFactor;
  }

  List<TierCombination> generateAllTierCombinations(ScrambleMetaSet metaset) {
    List<TierCombination> combinations = new ArrayList<>();
    List<Pair<String, String>> scrambles = new ArrayList<>();
    List<Integer> tierCounts = new ArrayList<>();
    for (ScrambleMeta meta : metaset) {
      String schemaName = meta.getSchemaName();
      String tableName = meta.getTableName();
      scrambles.add(Pair.of(schemaName, tableName));
      tierCounts.add(meta.getNumberOfTiers());
    }
    
    // create individual tier number sets; this will be the argument for the cartesian product
    List<Set<Integer>> tierLists = new ArrayList<>();
    for (int c : tierCounts) {
//      Set<Integer> tiers = Ranges.closedOpen(0, c).asSet(DiscreteDomains.integers());
      Set<Integer> tiers = new TreeSet<>();
      for (int i = 0; i < c; i++) {
        tiers.add(i);
      }
      tierLists.add(tiers);
    }
    Set<List<Integer>> product = Sets.cartesianProduct(tierLists);
    
    // convert the product to tier combination object
    for (List<Integer> c : product) {
      TierCombination comb = new TierCombination(scrambles, c);
      combinations.add(comb);
    }
    
    return combinations;
  }

  /**
   * Computes the scaling ratio for a certain tier combination, which is simply the inverse of the
   * sampling probability.
   *
   * The final sampling probability is the product of the sampling probabilities of the involving
   * tables. The sampling probability of each table can be computed by looking at a hypercube.
   *
   * @param tiers
   * @return
   */
  private double ratioOfAllCubes(TierCombination tiers, ScrambleMetaSet metaset) {
    double ratio = 0.0;
    for (HyperTableCube cube : cubes) {
      double r = ratioOfCube(cube, metaset, tiers);
      ratio += r;
    }
    return ratio;
  }

  private double ratioOfCube(HyperTableCube cube, ScrambleMetaSet metaset, TierCombination tiers) {
    double ratio = 1.0;
    for (Dimension dim : cube.getDimensions()) {
      String schemaName = dim.getSchemaName();
      String tableName = dim.getTableName();
      int tier = tiers.getTierNumberFor(schemaName, tableName);
      double ratioForDim = ratioOfDimension(dim, metaset, tier);
      ratio *= ratioForDim;
    }
    return ratio;
  }

  private double ratioOfDimension(Dimension dim, ScrambleMetaSet metaset, int tier) {
    String schemaName = dim.getSchemaName();
    String tableName = dim.getTableName();
    int begin = dim.getBegin();
    int end = dim.getEnd();
    ScrambleMeta meta = metaset.getSingleMeta(schemaName, tableName);
    List<Double> cumulDist = meta.getCumulativeDistributionForTier(tier);

    double ratio;
    if (begin == 0) {
      ratio = cumulDist.get(end);
    } else {
      ratio = cumulDist.get(end) - cumulDist.get(begin-1);
    }
    return ratio;
  }

  public List<String> getAggAlias() {
    return aggAlias;
  }
  
  public Map<SelectItem, List<ColumnOp>> getAggColumn() {
    return aggColumn;
  }
  
  public Map<Pair<String, UnnamedColumn>, String> getAggColumnAggAliasPair() {
    return aggColumnAggAliasPair;
  }
  
  public Map<Pair<String, UnnamedColumn>, String> getAggColumnAggAliasPairOfMaxMin() {
    return aggColumnAggAliasPairOfMaxMin;
  }
  
  public Map<ScrambleMeta, String> getTierColumnForScramble() {
    return tierColumnForScramble;
  }
  
  public void addCube(HyperTableCube cube) {
    cubes.add(cube);
  }
  
  public List<HyperTableCube> getCubes() {
    return cubes;
  }
  
  public Map<String, String> getMaxminAggAlias() {
    return maxminAggAlias;
  }
  
  public List<SelectItem> getOriginalSelectList() {
    return originalSelectList;
  }
  
  public String getTierColumnName() {
    return tierColumnName;
  }
  
  public void addAggAlias(String alias) {
    aggAlias.add(alias);
  }
  
  public void addMinAlias(String alias) {
    maxminAggAlias.put(alias, "min");
  }
  
  public void addMaxAlias(String alias) {
    maxminAggAlias.put(alias, "max");
  }
  
  public void setAggAlias(List<String> aggAlias) {
    this.aggAlias = aggAlias;
  }
  
  public void setAggColumn(Map<SelectItem, List<ColumnOp>> aggColumn) {
    this.aggColumn = aggColumn;
  }
  
  public void setAggColumnAggAliasPair(
      Map<Pair<String, UnnamedColumn>, String> aggColumnAggAliasPair) {
    this.aggColumnAggAliasPair = aggColumnAggAliasPair;
  }
  
  public void setAggColumnAggAliasPairOfMaxMin(
      Map<Pair<String, UnnamedColumn>, String> aggColumnAggAliasPairOfMaxMin) {
    this.aggColumnAggAliasPairOfMaxMin = aggColumnAggAliasPairOfMaxMin;
  }
  
  public void setCubes(List<HyperTableCube> cubes) {
    this.cubes = cubes;
  }
  
  public void setMaxminAggAlias(Map<String, String> maxminAggAlias) {
    this.maxminAggAlias = maxminAggAlias;
  }
  
  public void setOriginalSelectList(List<SelectItem> originalSelectList) {
    this.originalSelectList = originalSelectList;
  }
  
  public void setTierColumnName(String tierColumnName) {
    this.tierColumnName = tierColumnName;
  }
  
  public void addScrambleTableTierColumnAlias(ScrambleMeta meta, String alias) {
    tierColumnForScramble.put(meta, alias);
  }
  
  public void setTierColumnForScramble(
      Map<ScrambleMeta, String> tierColumnForScramble) {
    this.tierColumnForScramble = tierColumnForScramble;
  }
  
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE)
               .append("aggAliasPairs", aggColumnAggAliasPair)
               .append("tierColumns", tierColumnForScramble.values())
               .build();
  }
  
  public AggMeta deepcopy() {
    return new Cloner().deepClone(this);
  }
  
}
