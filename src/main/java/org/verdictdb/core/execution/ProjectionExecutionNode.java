package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.SelectQuery;

public class ProjectionExecutionNode extends QueryExecutionNode {

  List<SelectQuery> queries = new ArrayList<>();

  public ProjectionExecutionNode(
      DbmsConnection conn,
      List<SelectQuery> queries) {
    super(conn);
    this.queries = queries;
  }

  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    return null;
  }
}
