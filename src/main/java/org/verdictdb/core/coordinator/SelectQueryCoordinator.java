package org.verdictdb.core.coordinator;

import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.execution.ExecutablePlanRunner;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.sqlobject.SelectQuery;

public class SelectQueryCoordinator {

  DbmsConnection conn;

  public SelectQueryCoordinator(DbmsConnection conn) {
    this.conn = conn;
  }

  public SelectQuery standardizeQuery(String query) {
    // parse the query
    SelectQuery selectQuery = null;
    return selectQuery;
  }

  public ExecutionResultReader process(String query) {

    SelectQuery selectQuery = standardizeQuery(query);

    // make plan
    // if the plan does not include any aggregates, it will simply be a parsed structure of the original query.
    QueryExecutionPlan plan;

    // convert it to an asynchronous plan
    // if the plan does not include any aggregates, this operation should not alter the original plan.
    QueryExecutionPlan asyncPlan;

    // simplify the plan
    QueryExecutionPlan simplifiedAsyncPlan = null;

    // execute the plan
    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(conn, simplifiedAsyncPlan);

    return reader;
  }

}
