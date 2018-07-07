package org.verdictdb.core.querying.ola;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.UnnamedColumn;

/**
 *  Use for store hyper table cube and aggregate column alias name
 *  of individual aggregate node and combiner
 *
 */

public class AggMeta {

  List<HyperTableCube> cubes = new ArrayList<>();

  List<SelectItem> originalSelectList;

  List<String> aggAlias = new ArrayList<>();

  HashMap<SelectItem, List<ColumnOp>> aggColumn = new HashMap<>();

  HashMap<Pair<String, UnnamedColumn>, String> aggColumnAggAliasPair = new HashMap<>();

  public AggMeta() {}

  public List<String> getAggAlias() {
    return aggAlias;
  }

  public List<HyperTableCube> getCubes() {
    return cubes;
  }

  public void setAggAlias(List<String> aggAlias) {
    this.aggAlias = aggAlias;
  }

  public void setCubes(List<HyperTableCube> cubes) {
    this.cubes = cubes;
  }

  public HashMap<SelectItem, List<ColumnOp>> getAggColumn() {
    return aggColumn;
  }

  public List<SelectItem> getOriginalSelectList() {
    return originalSelectList;
  }

  public void setOriginalSelectList(List<SelectItem> originalSelectList) {
    this.originalSelectList = originalSelectList;
  }

  public HashMap<Pair<String, UnnamedColumn>, String> getAggColumnAggAliasPair() {
    return aggColumnAggAliasPair;
  }

  public void setAggColumn(HashMap<SelectItem, List<ColumnOp>> aggColumn) {
    this.aggColumn = aggColumn;
  }

  public void setAggColumnAggAliasPair(HashMap<Pair<String, UnnamedColumn>, String> aggColumnAggAliasPair) {
    this.aggColumnAggAliasPair = aggColumnAggAliasPair;
  }
}
