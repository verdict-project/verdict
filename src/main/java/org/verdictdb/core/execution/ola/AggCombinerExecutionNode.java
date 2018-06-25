package org.verdictdb.core.execution.ola;

import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.execution.CreateTableAsSelectExecutionNode;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.QueryExecutionNode;
import org.verdictdb.core.query.SelectQuery;

public class AggCombinerExecutionNode extends CreateTableAsSelectExecutionNode {

  private AggCombinerExecutionNode(String scratchpadSchemaName) {
    super(scratchpadSchemaName);
  }
  
  public static AggCombinerExecutionNode create(
      QueryExecutionNode queryExecutionNode,
      QueryExecutionNode queryExecutionNode2) {
    // TODO: combiner
    return null;
  }

  @Override
  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) {
    return ExecutionInfoToken.empty();
  }

}
