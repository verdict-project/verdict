/*
 *    Copyright 2018 University of Michigan
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

package org.verdictdb.coordinator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.DataTypeConverter;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.MetaDataProvider;
import org.verdictdb.connection.StaticMetaData;
import org.verdictdb.core.execplan.ExecutablePlanRunner;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.querying.QueryExecutionPlanFactory;
import org.verdictdb.core.querying.QueryExecutionPlanSimplifier;
import org.verdictdb.core.querying.ola.AsyncQueryExecutionPlan;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlreader.ScrambleTableReplacer;

public class SelectQueryCoordinator implements Coordinator {
  
  private ExecutablePlanRunner planRunner;

  DbmsConnection conn;

  ScrambleMetaSet scrambleMetaSet;

  String scratchpadSchema;

  SelectQuery lastQuery;

  VerdictOption options;
  
  private VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

  public SelectQueryCoordinator(DbmsConnection conn) {
    this(conn, new ScrambleMetaSet(), new VerdictOption());
  }

  public SelectQueryCoordinator(DbmsConnection conn, VerdictOption options) {
    this(conn, new ScrambleMetaSet(), options);
    this.options = options;
  }

  public SelectQueryCoordinator(
      DbmsConnection conn, ScrambleMetaSet scrambleMetaSet, VerdictOption options) {
    this(conn, scrambleMetaSet, options.getVerdictTempSchemaName());
    this.options = options;
  }

  public SelectQueryCoordinator(
      DbmsConnection conn, ScrambleMetaSet scrambleMetaSet, String scratchpadSchema) {
    this.conn = conn;
    this.scrambleMetaSet = scrambleMetaSet;
    this.scratchpadSchema = scratchpadSchema;
  }

  public ScrambleMetaSet getScrambleMetaSet() {
    return scrambleMetaSet;
  }

  public void setScrambleMetaSet(ScrambleMetaSet scrambleMetaSet) {
    this.scrambleMetaSet = scrambleMetaSet;
  }

  public SelectQuery getLastQuery() {
    return lastQuery;
  }

  /**
   * This method should only be used for testing.
   * Use process(String query, QueryContext context) for actual applications instead.
   */
  public ExecutionResultReader process(String query) throws VerdictDBException {
    
    return process(query, null);

//    SelectQuery selectQuery = standardizeQuery(query);
//
//    // make plan
//    // if the plan does not include any aggregates, it will simply be a parsed structure of the
//    // original query.
//    QueryExecutionPlan plan =
//        QueryExecutionPlanFactory.create(scratchpadSchema, scrambleMetaSet, selectQuery);
//
//    // convert it to an asynchronous plan
//    // if the plan does not include any aggregates, this operation should not alter the original
//    // plan.
//    QueryExecutionPlan asyncPlan = AsyncQueryExecutionPlan.create(plan);
//
//    // simplify the plan
//    //    QueryExecutionPlan simplifiedAsyncPlan = QueryExecutionPlanSimplifier.simplify(asyncPlan);
//    QueryExecutionPlanSimplifier.simplify2(asyncPlan);
//
//    //    asyncPlan.getRootNode().print();
//
//    // execute the plan
//    planRunner = new ExecutablePlanRunner(conn, asyncPlan);
//    ExecutionResultReader reader = planRunner.getResultReader();
//
//    lastQuery = selectQuery;
//
//    return reader;
  }

  public ExecutionResultReader process(String query, QueryContext context)
      throws VerdictDBException {

    SelectQuery selectQuery = standardizeQuery(query);

    // make plan
    // if the plan does not include any aggregates, it will simply be a parsed structure of the
    // original query.
    QueryExecutionPlan plan =
        QueryExecutionPlanFactory.create(scratchpadSchema, scrambleMetaSet, selectQuery, context);

    // convert it to an asynchronous plan
    // if the plan does not include any aggregates, this operation should not alter the original
    // plan.
    QueryExecutionPlan asyncPlan = AsyncQueryExecutionPlan.create(plan);
    log.debug("Async plan created.");

    // simplify the plan
    //    QueryExecutionPlan simplifiedAsyncPlan = QueryExecutionPlanSimplifier.simplify(asyncPlan);
    QueryExecutionPlanSimplifier.simplify2(asyncPlan);
    log.debug("Plan simplification done.");
    
    log.debug(asyncPlan.getRoot().getStructure());

    // execute the plan
    planRunner = new ExecutablePlanRunner(conn, asyncPlan);
    ExecutionResultReader reader = planRunner.getResultReader();

    lastQuery = selectQuery;

    return reader;
  }
  
  @Override
  public void abort() {
    log.debug(String.format("Closes %s.", this.getClass().getSimpleName()));
    planRunner.abort();
  }

  private SelectQuery standardizeQuery(String query) throws VerdictDBException {
    // parse the query
    RelationStandardizer.resetItemID();
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    SelectQuery relation = (SelectQuery) sqlToRelation.toRelation(query);
    MetaDataProvider metaData = createMetaDataFor(relation);
//    ScrambleMetaStore metaStore = new ScrambleMetaStore(conn, options);
    RelationStandardizer gen = new RelationStandardizer(metaData, conn.getSyntax());
    relation = gen.standardize(relation);

    ScrambleTableReplacer replacer = new ScrambleTableReplacer(scrambleMetaSet);
    replacer.replace(relation);

    return relation;
  }

  private MetaDataProvider createMetaDataFor(SelectQuery relation) throws VerdictDBException {
    StaticMetaData meta = new StaticMetaData();
    String defaultSchema = conn.getDefaultSchema();
    meta.setDefaultSchema(defaultSchema);

    // Extract all tables appeared in the query
    HashSet<BaseTable> tables = new HashSet<>();
    List<SelectQuery> queries = new ArrayList<>();
    queries.add(relation);
    while (!queries.isEmpty()) {
      SelectQuery query = queries.get(0);
      queries.remove(0);
      for (AbstractRelation t : query.getFromList()) {
        if (t instanceof BaseTable) tables.add((BaseTable) t);
        else if (t instanceof SelectQuery) queries.add((SelectQuery) t);
        else if (t instanceof JoinTable) {
          for (AbstractRelation join : ((JoinTable) t).getJoinList()) {
            if (join instanceof BaseTable) tables.add((BaseTable) join);
            else if (join instanceof SelectQuery) queries.add((SelectQuery) join);
          }
        }
      }
      if (query.getFilter().isPresent()) {
        UnnamedColumn where = query.getFilter().get();
        List<UnnamedColumn> toCheck = new ArrayList<>();
        toCheck.add(where);
        while (!toCheck.isEmpty()) {
          UnnamedColumn col = toCheck.get(0);
          toCheck.remove(0);
          if (col instanceof ColumnOp) {
            toCheck.addAll(((ColumnOp) col).getOperands());
          } else if (col instanceof SubqueryColumn) {
            queries.add(((SubqueryColumn) col).getSubquery());
          }
        }
      }
    }

    // Get table info from cached meta
    for (BaseTable t : tables) {
      List<Pair<String, String>> columns;
      StaticMetaData.TableInfo tableInfo;

      if (t.getSchemaName() == null) {
        columns = conn.getColumns(defaultSchema, t.getTableName());
        tableInfo = new StaticMetaData.TableInfo(defaultSchema, t.getTableName());
      } else {
        columns = conn.getColumns(t.getSchemaName(), t.getTableName());
        tableInfo = new StaticMetaData.TableInfo(t.getSchemaName(), t.getTableName());
      }
      List<Pair<String, Integer>> colInfo = new ArrayList<>();
      for (Pair<String, String> col : columns) {
        colInfo.add(
            new ImmutablePair<>(
                col.getLeft(), DataTypeConverter.typeInt(col.getRight().toLowerCase())));
      }
      meta.addTableData(tableInfo, colInfo);
    }

    return meta;
  }
}
