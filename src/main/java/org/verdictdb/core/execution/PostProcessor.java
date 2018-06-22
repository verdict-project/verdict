package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.query.SelectQueryOp;

public class PostProcessor extends QueryExecutionNode {

  public List<SelectQueryOp> queries = new ArrayList<>();

  public PostProcessor(DbmsConnection conn, List<SelectQueryOp> queries) {
    super(conn);
    this.queries = queries;
  }
  
  DbmsQueryResult process(List<DbmsQueryResult> intermediates) {
    return null;
  };

  @Override
  public void execute(BlockingDeque<ExecutionResult> resultQueue) {

  }
}
