package org.verdictdb.core.execution;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.DbmsMetadataCache;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.SelectQueryOp;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.core.sql.MetaData;
import org.verdictdb.core.sql.NonValidatingSQLParser;
import org.verdictdb.exception.UnexpectedTypeException;
import org.verdictdb.sql.syntax.SyntaxAbstract;

import com.google.common.base.Optional;

public class AggExecutionPlan {
  
  DbmsConnection conn;
  
  SelectQueryOp query;
  
  AggExecutionNode root;
  
  DbmsMetadataCache meta;
  
  /**
   * 
   * @param queryString A select query
   * @throws UnexpectedTypeException 
   */
  public AggExecutionPlan(DbmsConnection conn, SyntaxAbstract syntax, String queryString) throws UnexpectedTypeException {
    this(conn, syntax, (SelectQueryOp) new NonValidatingSQLParser().toRelation(queryString));
  }
  
  /**
   * 
   * @param query  A well-formed select query object
   * @throws UnexpectedTypeException
   */
  public AggExecutionPlan(DbmsConnection conn, SyntaxAbstract syntax, SelectQueryOp query) throws UnexpectedTypeException {
    this.conn = conn;
    this.meta = new DbmsMetadataCache(conn);
    if (!query.isAggregateQuery()) {
      throw new UnexpectedTypeException(query);
    }
    this.query = query;
    this.root = plan(conn, query);
  }
  
  /** 
   * Creates a tree in which each node is AggExecutionNode
   * 
   * @param conn
   * @param query
   * @return The root of the tree.
   */
  AggExecutionNode plan(DbmsConnection conn, SelectQueryOp query) {
    ScrambleMeta meta = new ScrambleMeta();
    return new AggExecutionNode(conn, meta, query);
  }
  
  public void execute(DbmsConnection conn) {
    
  }

}
