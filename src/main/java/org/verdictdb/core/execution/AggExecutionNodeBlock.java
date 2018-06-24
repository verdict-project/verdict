package org.verdictdb.core.execution;

import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.SelectQuery;

/**
 * Represents a group of execution nodes.
 * 
 * @author Yongjoo Park
 */
public class AggExecutionNodeBlock extends QueryExecutionNode {
  
  QueryExecutionNode root;

  public AggExecutionNodeBlock(DbmsConnection conn, SelectQuery query) {
    super(conn, query);
  }

  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    root.execute();
    return null;
  }

}
