package org.verdictdb.core.execution.ola;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.exception.VerdictDBValueException;

/**
 * Plans how to chop a big query into multiple small queries.
 * 
 * @author Yongjoo Park
 *
 */
public class AggBlockMeta {
  
//  int totalSequenceCount;
  
  List<HyperTableCube> cubes = new ArrayList<>();
  
  /**
   * 
   * @param scrambleMeta
   * @param scrambles   The scrambled tables that appear in a query.
   * @throws VerdictDBValueException
   */
  public AggBlockMeta(ScrambleMeta scrambleMeta, List<Pair<String, String>> scrambles) 
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
    
//    List<Pair<Integer, Integer>> remainingBlockSpans = null;
//    int totalBlockCount = computeBlockCount(remainingBlockSpans);
//    
//    while (totalBlockCount > 0) {
//      BlockSpans spans = new BlockSpans();
//      List<Pair<Integer, Integer>> singleBlock = createSingleBlock(remainingBlockSpans);
//      for (int i = 0; i < scrambles.size(); i++) {
//        String schemaName = scrambles.get(i).getLeft();
//        String tableName = scrambles.get(i).getRight();
//        spans.addEntry(schemaName, tableName, singleBlock.get(i).getLeft(), singleBlock.get(i).getRight());
//      }
//      remainingBlockSpans = subtractBlockSpans(remainingBlockSpans, singleBlock);
//      totalBlockCount -= computeBlockCount(singleBlock);
//    }
//    
//    if (scrambles.size() > 2) {
//      throw new VerdictDBValueException("The number of scrambled tables cannot be larger than two.");
//    }
//    
//    if (scrambles.size() == 1) {
//      String schemaName = scrambles.get(0).getLeft();
//      String tableName = scrambles.get(0).getRight();
//      totalSequenceCount = scrambleMeta.getAggregationBlockCount(schemaName, tableName);
//      
//      for (int i = 0; i < totalSequenceCount; i++) {
//        BlockSpans spans = new BlockSpans();
//        spans.addEntry(schemaName, tableName, i, i);
//        blockaggSpans.add(spans);
//      }
//      
//    }
//    else {
//      
//    }
  }
  
//  List<Pair<Integer, Integer>> subtractBlockSpans(List<Pair<Integer, Integer>> remainingBlockSpans,
//      List<Pair<Integer, Integer>> singleBlock) {
//    // TODO Auto-generated method stub
//    return null;
//  }
//
//  int computeBlockCount(List<Pair<Integer, Integer>> blockSpans) {
//    int blockCount = 1;
//    for (Pair<Integer, Integer> span : blockSpans) {
//      blockCount *= span.getRight() - span.getLeft() + 1;
//    }
//    return blockCount;
//  }

//  /**
//   * 
//   * @param blockSpans Remaining block spans
//   * @return
//   */
//  List<Pair<Integer, Integer>> createSingleBlock(List<Pair<Integer, Integer>> blockSpans) {
//    List<Pair<Integer, Integer>> singleBlockSpan = new ArrayList<>();
//    
//    
//    return singleBlockSpan;
//  }

  public int totalBlockAggCount() {
    return cubes.size();
  }

  public Pair<Integer, Integer> getAggBlockSpanForTable(String schemaName, String tableName, int sequence) {
    HyperTableCube cube = cubes.get(sequence);
    return cube.getSpanOf(schemaName, tableName);
  }

}


//class BlockSpansSeries {
//  
//  List<BlockSpans> series = new ArrayList<>();
//  
//  public BlockSpansSeries() {}
//  
//  public BlockSpans getBlockSpans(int index) {
//    return series.get(index);
//  }
//  
//  public List<BlockSpans> getSeries() {
//    return series;
//  }
//  
//  public void add(BlockSpans spans) {
//    series.add(spans);
//  }
//
//  BlockSpans joinToLeft(BlockSpansSeries leftSeries) throws VerdictDBValueException {
//    if (leftSeries.getBlockSpans(0).getTables().size() != 1) {
//      throw new VerdictDBValueException("The block span to be joined must involve a single table.");
//    }
//    BlockSpansSeries joined = new BlockSpansSeries();
//    
//    int startIndex = 0;
//    int leftEnd = leftSeries.getSeries().size();
//    int rightEnd = series.size();
//    
//    // horizontal block: all blocks from the left is joined to the first block of the right
//    {
//      List<BlockSpans> leftBlocks = leftSeries.getSeries().subList(startIndex, leftEnd);
//      BlockSpans rightBlock = series.get(startIndex);
//      for (BlockSpans leftBlock : leftBlocks) {
//        BlockSpans joinedSpans = new BlockSpans();
//        
//        // left table component
//        Pair<String, String> leftFullName = leftBlock.getTables().iterator().next();
//        String schemaName = leftFullName.getLeft();
//        String tableName = leftFullName.getRight();
//        Pair<Integer, Integer> span = leftBlock.getSpanOf(schemaName, tableName);
//        joinedSpans.addEntry(schemaName, tableName, span.getLeft(), span.getRight());
//
//        // right table components (one iter for each table)
//        for (Pair<String, String> rightFullName : rightBlock.getTables()) {
//          String rightSchemaName = rightFullName.getLeft();
//          String rightTableName = rightFullName.getRight();
//          Pair<Integer, Integer> rightSpan = rightBlock.getSpanOf(rightSchemaName, rightTableName);
//          joinedSpans.addEntry(rightSchemaName, rightTableName, rightSpan.getLeft(), rightSpan.getRight());
//        }
//
//        joined.add(joinedSpans);
//      }
//    }
//    
//    // vertical block: the first block of the left is joined to all blocks from the right.
//    {
//      BlockSpans leftBlock = leftSeries.getSeries().get(startIndex);
//      List<BlockSpans> rightBlocks = series.subList(startIndex, rightEnd);
//      for (BlockSpans rightBlock : rightBlocks) {
//        BlockSpans joinedSpans = new BlockSpans();
//        
//        // left table component
//        Pair<String, String> leftFullName = leftBlock.getTables().iterator().next();
//        String schemaName = leftFullName.getLeft();
//        String tableName = leftFullName.getRight();
//        Pair<Integer, Integer> span = leftBlock.getSpanOf(schemaName, tableName);
//        joinedSpans.addEntry(schemaName, tableName, span.getLeft(), span.getRight());
//        
//        // right table component
//        for (Pair<String, String> rightFullName : rightBlock.getTables()) {
//          String rightSchemaName = rightFullName.getLeft();
//          String rightTableName = rightFullName.getRight();
//          Pair<Integer, Integer> rightSpan = rightBlock.getSpanOf(rightSchemaName, rightTableName);
//          joinedSpans.addEntry(rightSchemaName, rightTableName, rightSpan.getLeft(), rightSpan.getRight());
//        }
//        
//        joined.add(joinedSpans);
//      }
//    }
//    
//    return null;
//  }
//  
//}
//
//class BlockSpans {
//  
//  // pairs of (schemaName, tableName) and (blockStart, blockEnd)
//  Map<Pair<String, String>, Pair<Integer, Integer>> tablesToSpan = new HashMap<>();
//  
//  public Set<Pair<String, String>> getTables() {
//    return tablesToSpan.keySet();
//  }
//  
//  public void addEntry(String schemaName, String tableName, int beginBlock, int endBlock) {
//    tablesToSpan.put(Pair.of(schemaName, tableName), Pair.of(beginBlock, endBlock));
//  }
//  
//  public Pair<Integer, Integer> getSpanOf(String schemaName, String tableName) {
//    return tablesToSpan.get(Pair.of(schemaName, tableName));
//  }
//  
//}
