package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.aggresult.AggregateFrame;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.SelectQueryOp;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.core.rewriter.aggresult.AggResultCombiner;
import org.verdictdb.core.rewriter.aggresult.SingleAggResultRewriter;
import org.verdictdb.core.rewriter.query.AggQueryRewriter;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.result.AsyncHandler;

/**
 * Represents an execution of a single aggregate query (without nested components).
 * 
 * @author Yongjoo Park
 *
 */
public class AggExecutionNode {
  
  DbmsConnection conn;
  
  ScrambleMeta meta;
  
  List<String> nonaggColumns = new ArrayList<>();
  
  /**
   * Pairs of aggregate column name and its aggregation type
   */
  List<Pair<String, String>> aggColumns = new ArrayList<>();
  
  SelectQueryOp originalQuery;
  
//  SelectQueryOp rewrittenQuery;
  
  public AggExecutionNode(DbmsConnection conn, ScrambleMeta meta, SelectQueryOp query) {
    this.conn = conn;
    this.meta = meta;
    this.originalQuery = query;
  }
  
  public void asyncExecute(AsyncHandler handler) throws VerdictDbException {
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewrittenQueries = rewriter.rewrite(originalQuery);
    AggregateFrame combinedAggResult = null;
    for (int i = 0; i < rewrittenQueries.size(); i++) {
      AggregateFrame newAggResult = null;
      if (i == 0) {
        combinedAggResult = newAggResult;
      }
      else {
        combinedAggResult = AggResultCombiner.combine(combinedAggResult, newAggResult);
      }
      SingleAggResultRewriter aggRewriter = new SingleAggResultRewriter(combinedAggResult);
    }
    
    
  }
  
  public DbmsQueryResult execute() {
    return null;
  }
  
}

