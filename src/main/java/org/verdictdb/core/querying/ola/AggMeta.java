package org.verdictdb.core.querying.ola;


import java.util.ArrayList;
import java.util.List;

/**
 *  Use for store hyper table cube and aggregate column alias name
 *  of individual aggregate node and combiner
 *
 */

public class AggMeta {

  List<HyperTableCube> cubes = new ArrayList<>();

  List<String> aggAlias = new ArrayList<>();

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
}
