package org.verdictdb.core.coordinator;

import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.StaticMetaData;
import org.verdictdb.core.execution.ExecutablePlanRunner;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.querying.QueryExecutionPlanSimplifier;
import org.verdictdb.core.querying.ola.AsyncQueryExecutionPlan;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;

public class SelectQueryCoordinator {

  DbmsConnection conn;

  ScrambleMetaSet scrambleMetaSet;

  StaticMetaData staticMetaData;

  public SelectQueryCoordinator(DbmsConnection conn) {
    this.conn = conn;
  }

  public SelectQuery standardizeQuery(String query) throws VerdictDBException {
    // parse the query
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery relation = (SelectQuery) sqlToRelation.toRelation(query);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize(relation);
    return relation;
  }

  public ScrambleMetaSet getScrambleMetaSet() {
    return scrambleMetaSet;
  }

  public void setScrambleMetaSet(ScrambleMetaSet scrambleMetaSet) {
    this.scrambleMetaSet = scrambleMetaSet;
  }

  public StaticMetaData getStaticMetaData() {
    return staticMetaData;
  }

  public void setStaticMetaData(StaticMetaData staticMetaData) {
    this.staticMetaData = staticMetaData;
  }

  public ExecutionResultReader process(String query) throws VerdictDBException {

    SelectQuery selectQuery = standardizeQuery(query);

    // make plan
    // if the plan does not include any aggregates, it will simply be a parsed structure of the original query.
    QueryExecutionPlan plan = new QueryExecutionPlan("verdictdb_temp", scrambleMetaSet, selectQuery);;

    // convert it to an asynchronous plan
    // if the plan does not include any aggregates, this operation should not alter the original plan.
    QueryExecutionPlan asyncPlan = AsyncQueryExecutionPlan.create(plan);;

    // simplify the plan
    QueryExecutionPlan simplifiedAsyncPlan = QueryExecutionPlanSimplifier.simplify(asyncPlan);

    // execute the plan
    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(conn, simplifiedAsyncPlan);

    return reader;
  }

}
