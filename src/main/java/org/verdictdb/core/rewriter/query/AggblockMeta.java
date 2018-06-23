package org.verdictdb.core.rewriter.query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;

/**
 * 
 * 
 * This class effectively has the following information.
 * [ { "tier": number,
 *     "total": 
 *   } ...
 * ]
 * 
 * @author Yongjoo Park
 *
 */
public class AggblockMeta implements Iterable<Entry<Pair<String, String>, Pair<Integer, Integer>>> {
  
  Map<Pair<String, String>, Pair<Integer, Integer>> data = new HashMap<>();
  
  public static AggblockMeta empty() {
    return new AggblockMeta();
  }
  
  /**
   * 
   * @param schemaName
   * @param tableName
   * @param blockSpan Inclusive range on both ends
   */
  public void addMeta(String schemaName, String tableName, Pair<Integer, Integer> blockSpan) {
    data.put(Pair.of(schemaName, tableName), blockSpan);
  }

  @Override
  public Iterator<Entry<Pair<String, String>, Pair<Integer, Integer>>> iterator() {
    return data.entrySet().iterator();
  }

}
