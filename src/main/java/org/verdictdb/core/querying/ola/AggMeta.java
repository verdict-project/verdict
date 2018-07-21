package org.verdictdb.core.querying.ola;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.UnnamedColumn;

/**
 *  Use for store hyper table cube and aggregate column alias name
 *  of individual aggregate node and combiner
 *
 */
public class AggMeta implements Serializable {

  private static final long serialVersionUID = 3186577687141707687L;

  List<HyperTableCube> cubes = new ArrayList<>();

  List<SelectItem> originalSelectList;

  List<String> aggAlias = new ArrayList<>();

  HashMap<String, String> maxminAggAlias = new HashMap<>();

  HashMap<SelectItem, List<ColumnOp>> aggColumn = new HashMap<>();

  // (agg function, argument), alias
  HashMap<Pair<String, UnnamedColumn>, String> aggColumnAggAliasPair = new HashMap<>();

  HashMap<Pair<String, UnnamedColumn>, String> aggColumnAggAliasPairOfMaxMin = new HashMap<>();
  
  String tierColumnName;

  public AggMeta() {}

  public List<String> getAggAlias() {
    return aggAlias;
  }

  public HashMap<SelectItem, List<ColumnOp>> getAggColumn() {
    return aggColumn;
  }

  public HashMap<Pair<String, UnnamedColumn>, String> getAggColumnAggAliasPair() {
    return aggColumnAggAliasPair;
  }

  public HashMap<Pair<String, UnnamedColumn>, String> getAggColumnAggAliasPairOfMaxMin() {
    return aggColumnAggAliasPairOfMaxMin;
  }

  public List<HyperTableCube> getCubes() {
    return cubes;
  }

  public HashMap<String, String> getMaxminAggAlias() {
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

  public void setAggColumn(HashMap<SelectItem, List<ColumnOp>> aggColumn) {
    this.aggColumn = aggColumn;
  }

  public void setAggColumnAggAliasPair(HashMap<Pair<String, UnnamedColumn>, String> aggColumnAggAliasPair) {
    this.aggColumnAggAliasPair = aggColumnAggAliasPair;
  }

  public void setAggColumnAggAliasPairOfMaxMin(HashMap<Pair<String, UnnamedColumn>, String> aggColumnAggAliasPairOfMaxMin) {
    this.aggColumnAggAliasPairOfMaxMin = aggColumnAggAliasPairOfMaxMin;
  }

  public void setCubes(List<HyperTableCube> cubes) {
    this.cubes = cubes;
  }

  public void setMaxminAggAlias(HashMap<String, String> maxminAggAlias) {
    this.maxminAggAlias = maxminAggAlias;
  }

  public void setOriginalSelectList(List<SelectItem> originalSelectList) {
    this.originalSelectList = originalSelectList;
  }

  public void setTierColumnName(String tierColumnName) {
    this.tierColumnName = tierColumnName;
  }
  
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE)
        .append("aggAliasPairs", aggColumnAggAliasPair)
//        .append("cubes", cubes)
        .build();
  }
}
