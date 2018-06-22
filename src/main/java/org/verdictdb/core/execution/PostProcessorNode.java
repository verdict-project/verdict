package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.query.SelectQuery;

public class PostProcessorNode extends QueryExecutionNode {

  List<SelectQuery> queries = new ArrayList<>();

  public PostProcessorNode(DbmsConnection conn, List<SelectQuery> queries) {
    super(conn);
    this.queries = queries;
  }

  @Override
  public void executeNode(
      List<ExecutionResult> resultFromChildren, 
      BlockingDeque<ExecutionResult> resultQueue) {
    // TODO Auto-generated method stub
  }
}
