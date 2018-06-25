package org.verdictdb.core.execution.ola;

import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.execution.ExecutionResult;
import org.verdictdb.core.execution.QueryExecutionNode;
import org.verdictdb.core.query.SelectQuery;

public class AggCombinerExecutionNode extends QueryExecutionNode {

  private AggCombinerExecutionNode(SelectQuery query) {
    super(query);
  }
  
  public static AggCombinerExecutionNode create(
      QueryExecutionNode queryExecutionNode,
      QueryExecutionNode queryExecutionNode2) {
    // TODO: combiner
    return null;
  }

  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    return ExecutionResult.empty();
  }

}
