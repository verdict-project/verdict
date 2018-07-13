package org.verdictdb.core.querying.ola;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.exception.VerdictDBValueException;

/**
 * Plans how to chop a big query into multiple small queries.
 * 
 * @author Yongjoo Park
 *
 */
public class OlaAggregationPlan {
  
  List<HyperTableCube> cubes = new ArrayList<>();
  
  // alias name for aggregate item and their aggregate type
  
  /**
   * 
   * @param scrambleMeta
   * @param scrambles   The scrambled tables that appear in a query.
   * @throws VerdictDBValueException
   */
  public OlaAggregationPlan(ScrambleMetaSet scrambleMeta, List<Pair<String, String>> scrambles) 
      throws VerdictDBValueException {
    
    // exception checks
    if (scrambles.size() == 0) {
      return;
    }
    if ((new HashSet<>(scrambles)).size() < scrambles.size()) {
      throw new VerdictDBValueException("The same scrambled table cannot be included more than once.");
    }
    
    // construct a cube for slicing
    List<Dimension> dims = new ArrayList<>();
    for (Pair<String, String> fullTableName : scrambles) {
      String schemaName = fullTableName.getLeft();
      String tableName = fullTableName.getRight();
      int aggBlockCount = scrambleMeta.getAggregationBlockCount(schemaName, tableName);
      dims.add(new Dimension(schemaName, tableName, 0, aggBlockCount-1));
    }
    HyperTableCube originalCube = new HyperTableCube(dims);
    
    // slice
    cubes = originalCube.roundRobinSlice();
    
  }
  
  // TODO: use this method to create a merged metadata
  // this method is supposed to rely on HyperTableCube's merge method.
  public static OlaAggregationPlan createMergedOlaAggMeta(
      OlaAggregationPlan meta1, 
      OlaAggregationPlan meta2) {
    
    return null;
  }
  
  public int totalBlockAggCount() {
    return cubes.size();
  }

  public Pair<Integer, Integer> getAggBlockSpanForTable(String schemaName, String tableName, int sequence) {
    HyperTableCube cube = cubes.get(sequence);
    return cube.getSpanOf(schemaName, tableName);
  }

}

