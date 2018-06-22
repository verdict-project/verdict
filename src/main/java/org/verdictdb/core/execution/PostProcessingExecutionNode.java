package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.SelectQuery;

public class PostProcessingExecutionNode extends QueryExecutionNode {

  List<SelectQuery> queries = new ArrayList<>();

  public PostProcessingExecutionNode(
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
