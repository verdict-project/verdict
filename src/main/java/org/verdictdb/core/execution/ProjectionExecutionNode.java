package org.verdictdb.core.execution;

import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.SelectQuery;

public class ProjectionExecutionNode extends QueryExecutionNode {

  public ProjectionExecutionNode(
      DbmsConnection conn,
      SelectQuery query) {
    super(conn, query);
  }

  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    return null;
  }
}
