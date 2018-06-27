package org.verdictdb.core.execution;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.resulthandler.AsyncHandler;

import java.util.List;

public class QueryReportNode extends QueryExecutionNode {

  AsyncHandler handler;

  public QueryReportNode(QueryExecutionPlan plan){
    super(plan);
    this.addDependency(plan.root);
    ExecutionTokenQueue queue = generateListeningQueue();
    this.dependents.get(0).addBroadcastingQueue(queue);
  }

  @Override
  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) throws VerdictDBException {
    for (ExecutionInfoToken result:downstreamResults) {
      DbmsQueryResult queryResult = (DbmsQueryResult) result.getValue("queryResult");
      handler.handle(queryResult);
    }
    return null;
  }

  @Override
  public QueryExecutionNode deepcopy() {
    return null;
  }

  public void setHandler(AsyncHandler handler) { this.handler = handler; }
}
