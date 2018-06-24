package org.verdictdb.core.rewriter.aggresult;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.aggresult.AggregateFrame;
import org.verdictdb.core.rewriter.query.AggblockMeta;

public class AggResultCombiner {
  
  public static Pair<AggregateFrame, AggblockMeta> combine(
      AggregateFrame agg1,
      AggblockMeta aggblockMeta1,
      AggregateFrame agg2, 
      AggblockMeta aggblockMeta2) {
    return Pair.of(agg2, aggblockMeta2);
  }

  public static Pair<AggregateFrame, AggblockMeta> scaleSingle(AggregateFrame left, AggblockMeta right) {
    // TODO Auto-generated method stub
    return null;
  }

}
