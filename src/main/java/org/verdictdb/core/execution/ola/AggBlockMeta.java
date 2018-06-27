package org.verdictdb.core.execution.ola;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.exception.VerdictDBValueException;

public class AggBlockMeta {
  
  int totalSequenceCount;
  
  List<BlockSpans> blockaggSpans = new ArrayList<>();
  
  public AggBlockMeta(ScrambleMeta scrambleMeta, List<Pair<String, String>> scrambles) throws VerdictDBValueException {
    if (scrambles.size() == 0) {
      // do nothing
    } else if (scrambles.size() == 1) {
      String schemaName = scrambles.get(0).getLeft();
      String tableName = scrambles.get(0).getRight();
      totalSequenceCount = scrambleMeta.getAggregationBlockCount(schemaName, tableName);
      
      for (int i = 0; i < totalSequenceCount; i++) {
        BlockSpans spans = new BlockSpans();
        spans.addEntry(schemaName, tableName, i, i);
        blockaggSpans.add(spans);
      }
      
    } else if (scrambles.size() == 2) {
      throw new VerdictDBValueException("Not implemented yet.");
    } else {
      throw new VerdictDBValueException("The number of scrambled tables cannot be larger than two.");
    }
  }

  public int totalBlockAggCount() {
    return totalSequenceCount;
  }

  public Pair<Integer, Integer> getAggBlockSpanForTable(String schemaName, String tableName, int sequence) {
    BlockSpans spans = blockaggSpans.get(sequence);
    return spans.getSpanOf(schemaName, tableName);
  }

}

class BlockSpans {
  
  // pairs of (schemaName, tableName) and (blockStart, blockEnd)
  Map<Pair<String, String>, Pair<Integer, Integer>> tablesToSpan = new HashMap<>();
  
  public void addEntry(String schemaName, String tableName, int beginBlock, int endBlock) {
    tablesToSpan.put(Pair.of(schemaName, tableName), Pair.of(beginBlock, endBlock));
  }
  
  public Pair<Integer, Integer> getSpanOf(String schemaName, String tableName) {
    return tablesToSpan.get(Pair.of(schemaName, tableName));
  }
}
