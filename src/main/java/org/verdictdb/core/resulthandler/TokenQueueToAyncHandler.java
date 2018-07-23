/*
 *    Copyright 2017 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.core.resulthandler;
// package org.verdictdb.resulthandler;
//
// import org.verdictdb.core.connection.DbmsQueryResult;
// import org.verdictdb.core.execution.ExecutionInfoToken;
// import org.verdictdb.core.execution.ExecutionTokenQueue;
// import org.verdictdb.core.querynode.QueryExecutionPlan;
//
// public class TokenQueueToAyncHandler {
//
//  AsyncHandler handler;
//
//  ExecutionTokenQueue listeningQueue = new ExecutionTokenQueue();
//
//  QueryExecutionPlan plan;
//
//  public TokenQueueToAyncHandler(QueryExecutionPlan plan, ExecutionTokenQueue queue){
//    this.plan = plan;
//    this.listeningQueue = queue;
//    this.plan.getRoot().addBroadcastingQueue(queue);
//  }
//
//  public void execute() {
//    ExecutionInfoToken result;
//    while (this.plan.getRoot().getStatus().equals("running")) {
//      result = listeningQueue.take();
//      DbmsQueryResult queryResult = (DbmsQueryResult) result.getValue("queryResult");
//      handler.handle(queryResult);
//    }
//    result = listeningQueue.poll();
//    while (result != null) {
//      if (result.isFailureToken() || result.isSuccessToken()) {
//        break;
//      }
//      DbmsQueryResult queryResult = (DbmsQueryResult) result.getValue("queryResult");
//      handler.handle(queryResult);
//      result = listeningQueue.poll();
//    }
//  }
//
//  public void setHandler(AsyncHandler handler) { this.handler = handler; }
// }
