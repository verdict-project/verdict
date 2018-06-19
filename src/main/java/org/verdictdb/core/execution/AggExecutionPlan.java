package org.verdictdb.core.execution;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.SelectQueryOp;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.exception.UnexpectedTypeException;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.sql.syntax.SyntaxAbstract;

import com.google.common.base.Optional;

public class AggExecutionPlan {
  
  Optional<String> queryString;
  
  SelectQueryOp query;
  
  AggExecutionNode root;
  
  /**
   * 
   * @param query A select query
   */
  public AggExecutionPlan(DbmsConnection conn, SyntaxAbstract syntax, String query) {
    this.queryString = Optional.of(query);
    
    // converts a query string into a select query.
    // we will need to pull some metadata from the underlying database.
  }
  
  /**
   * 
   * @param query  A well-formed select query object
   * @throws VerdictDbException 
   */
  public AggExecutionPlan(DbmsConnection conn, SelectQueryOp query) throws VerdictDbException {
    if (!query.isAggregateQuery()) {
      throw new UnexpectedTypeException(query);
    }
    this.query = query;
    plan(conn, query);
  }
  
  void plan(DbmsConnection conn, SelectQueryOp query) throws VerdictDbException {
    ScrambleMeta meta = new ScrambleMeta();
    root = new AggExecutionNode(conn, meta, query);
  }
  
  public void execute(DbmsConnection conn) {
    
  }

}
