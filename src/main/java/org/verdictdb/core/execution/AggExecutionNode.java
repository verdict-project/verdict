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
import org.verdictdb.core.sql.SelectQueryToSql;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.result.AsyncHandler;
import org.verdictdb.sql.syntax.HiveSyntax;

/**
 * Represents an execution of a single aggregate query (without nested components).
 * 
 * @author Yongjoo Park
 *
 */
public class AggExecutionNode {
  
  DbmsConnection conn;
  
  ScrambleMeta meta;
  
  // group-by columns
  List<String> nonaggColumns = new ArrayList<>();
  
  // agg columns. pairs of their column names and their types (i.e., sum, avg, count)
  List<Pair<String, String>> aggColumns = new ArrayList<>();
  
  SelectQueryOp originalQuery;
  
//  SelectQueryOp rewrittenQuery;
  
  public AggExecutionNode(DbmsConnection conn, ScrambleMeta meta, SelectQueryOp query) {
    this.conn = conn;
    this.meta = meta;
    this.originalQuery = query;
  }
  
  public DbmsQueryResult singleExecute() throws VerdictDbException {
    AggQueryRewriter aggQueryRewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> aggWithErrorQueries = aggQueryRewriter.rewrite(originalQuery);
    
    AbstractRelation q = aggWithErrorQueries.get(0);
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String query_string = relToSql.toSql(q);
    
    DbmsQueryResult rawResult = conn.executeQuery(query_string);
    AggregateFrame newAggResult = AggregateFrame.fromDmbsQueryResult(rawResult, nonaggColumns, aggColumns);
    
    SingleAggResultRewriter aggResultRewriter = new SingleAggResultRewriter(newAggResult);
    AggregateFrame rewritten = aggResultRewriter.rewrite(nonaggColumns, aggColumns);
    DbmsQueryResult resultToUser = rewritten.toDbmsQueryResult();
    return resultToUser;
  }
  
  public void asyncExecute(AsyncHandler handler) throws VerdictDbException {
    AggQueryRewriter aggQueryRewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> aggWithErrorQueries = aggQueryRewriter.rewrite(originalQuery);
    AggregateFrame combinedAggResult = null;
    
    // execute the rewritten queries one by one
    for (int i = 0; i < aggWithErrorQueries.size(); i++) {
      AbstractRelation q = aggWithErrorQueries.get(i);
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String query_string = relToSql.toSql(q);
      
      DbmsQueryResult rawResult = conn.executeQuery(query_string);
      AggregateFrame newAggResult = AggregateFrame.fromDmbsQueryResult(rawResult, nonaggColumns, aggColumns);
      
      // combine with previous answers
      if (i == 0) {
        combinedAggResult = newAggResult;
      }
      else {
        combinedAggResult = AggResultCombiner.combine(combinedAggResult, newAggResult);
      }
      
      // convert to a user-friendly answer
      SingleAggResultRewriter aggResultRewriter = new SingleAggResultRewriter(combinedAggResult);
      AggregateFrame rewritten = aggResultRewriter.rewrite(nonaggColumns, aggColumns);
      DbmsQueryResult resultToUser = rewritten.toDbmsQueryResult();
      handler.handle(resultToUser);
    }
  }
  
  public DbmsQueryResult execute() {
    return null;
  }
  
}

