package org.verdictdb.core.execution;

import java.util.List;

import org.verdictdb.connection.DbmsConnection;

public class AggCombinerExecutionNode extends QueryExecutionNode {

  public AggCombinerExecutionNode(DbmsConnection conn) {
    super(conn, null);
  }

  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    return ExecutionResult.empty();
  }

}
