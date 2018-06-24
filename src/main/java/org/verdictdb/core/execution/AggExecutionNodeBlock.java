package org.verdictdb.core.execution;

import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.SelectQuery;

/**
 * Contains the references to the ExecutionNodes that contain scrambled tables. This does not include
 * the scrambled tables in sub aggregate queries.
 * 
 * @author Yongjoo Park
 *
 */
public class AggExecutionNodeBlock extends QueryExecutionNode {
  
  QueryExecutionNode root;

  public AggExecutionNodeBlock(DbmsConnection conn, QueryExecutionNode root) {
    super(conn, null);
  }
  
  public QueryExecutionNode getRoot() {
    return root;
  }
  
  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * 
   * @param nodeBlock
   * @return Returns the root of the multiple aggregation nodes (each of which involves different combinations
   * of partitions)
   */
  public QueryExecutionNode constructProgressiveAggNodes() {
    return null;
  }
  
  public QueryExecutionNode deepcopyExcludingDependentAggregates() {
    return null;
  }

}
