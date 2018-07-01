package org.verdictdb.resulthandler;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.querynode.QueryExecutionPlan;

public class TokenQueueToAyncHandler {

  AsyncHandler handler;

  ExecutionTokenQueue listeningQueue = new ExecutionTokenQueue();

  QueryExecutionPlan plan;

  public TokenQueueToAyncHandler(QueryExecutionPlan plan, ExecutionTokenQueue queue){
    this.plan = plan;
    this.listeningQueue = queue;
    this.plan.getRoot().addBroadcastingQueue(queue);
  }

  public void execute() {
    ExecutionInfoToken result;
    while (this.plan.getRoot().getStatus().equals("running")) {
      result = listeningQueue.take();
      DbmsQueryResult queryResult = (DbmsQueryResult) result.getValue("queryResult");
      handler.handle(queryResult);
    }
    result = listeningQueue.poll();
    while (result != null) {
      if (result.isFailureToken() || result.isSuccessToken()) {
        break;
      }
      DbmsQueryResult queryResult = (DbmsQueryResult) result.getValue("queryResult");
      handler.handle(queryResult);
      result = listeningQueue.poll();
    }
  }

  public void setHandler(AsyncHandler handler) { this.handler = handler; }
}
