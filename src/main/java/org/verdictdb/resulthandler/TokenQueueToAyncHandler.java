package org.verdictdb.resulthandler;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.execution.QueryExecutionNode;
import org.verdictdb.core.execution.QueryExecutionPlan;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.resulthandler.AsyncHandler;

import java.util.List;

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
