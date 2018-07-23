/*
 *    Copyright 2017 University of Michigan
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

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.exception.VerdictDBValueException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Plans how to chop a big query into multiple small queries.
 *
 * @author Yongjoo Park
 */
public class OlaAggregationPlan {

  List<HyperTableCube> cubes = new ArrayList<>();

  // alias name for aggregate item and their aggregate type

  /**
   * @param scrambleMeta
   * @param scrambles The scrambled tables that appear in a query.
   * @throws VerdictDBValueException
   */
  public OlaAggregationPlan(ScrambleMetaSet scrambleMeta, List<Pair<String, String>> scrambles)
      throws VerdictDBValueException {

    // exception checks
    if (scrambles.size() == 0) {
      return;
    }
    if ((new HashSet<>(scrambles)).size() < scrambles.size()) {
      throw new VerdictDBValueException(
          "The same scrambled table cannot be included more than once.");
    }

    // construct a cube for slicing
    List<Dimension> dims = new ArrayList<>();
    for (Pair<String, String> fullTableName : scrambles) {
      String schemaName = fullTableName.getLeft();
      String tableName = fullTableName.getRight();
      int aggBlockCount = scrambleMeta.getAggregationBlockCount(schemaName, tableName);
      dims.add(new Dimension(schemaName, tableName, 0, aggBlockCount - 1));
    }
    HyperTableCube originalCube = new HyperTableCube(dims);

    // slice
    cubes = originalCube.roundRobinSlice();
  }

  // TODO: use this method to create a merged metadata
  // this method is supposed to rely on HyperTableCube's merge method.
  public static OlaAggregationPlan createMergedOlaAggMeta(
      OlaAggregationPlan meta1, OlaAggregationPlan meta2) {

    return null;
  }

  public int totalBlockAggCount() {
    return cubes.size();
  }

  public Pair<Integer, Integer> getAggBlockSpanForTable(
      String schemaName, String tableName, int sequence) {
    HyperTableCube cube = cubes.get(sequence);
    return cube.getSpanOf(schemaName, tableName);
  }
}
