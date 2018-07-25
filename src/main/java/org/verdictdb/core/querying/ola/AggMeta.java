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
import java.util.Map;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.UnnamedColumn;

/**
 * Use for store hyper table cube and aggregate column alias name of individual aggregate node and
 * combiner
 */
public class AggMeta implements Serializable {

  private static final long serialVersionUID = 3186577687141707687L;

  // List of covered blocks
  List<HyperTableCube> cubes = new ArrayList<>();

  List<SelectItem> originalSelectList;

  List<String> aggAlias = new ArrayList<>();

  Map<ScrambleMeta, String> scrambleTableTierColumnAlias = new HashMap<>();
  
  Map<String, String> maxminAggAlias = new HashMap<>();
  
  Map<SelectItem, List<ColumnOp>> aggColumn = new HashMap<>();

  // (agg function, argument), alias
  Map<Pair<String, UnnamedColumn>, String> aggColumnAggAliasPair = new HashMap<>();
  
  Map<Pair<String, UnnamedColumn>, String> aggColumnAggAliasPairOfMaxMin = new HashMap<>();

  String tierColumnName;

  public AggMeta() {}

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

  public Map<ScrambleMeta, String> getScrambleTableTierColumnAlias() {
    return scrambleTableTierColumnAlias;
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

  public void setScrambleTableTierColumnAlias(
      Map<ScrambleMeta, String> scrambleTableTierColumnAlias) {
    this.scrambleTableTierColumnAlias = scrambleTableTierColumnAlias;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE)
        .append("aggAliasPairs", aggColumnAggAliasPair)
        //        .append("cubes", cubes)
        .build();
  }
}
