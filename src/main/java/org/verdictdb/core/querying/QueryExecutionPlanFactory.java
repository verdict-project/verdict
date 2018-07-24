package org.verdictdb.core.querying;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;

public class QueryExecutionPlanFactory {
  
  /**
   * Creates a node tree and return it as an instance of QueryExecutionPlan.
   * @param query
   * @return
   */
  public static QueryExecutionPlan create(
      String scratchpadSchemaName) {
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan();
    queryExecutionPlan.idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
    return queryExecutionPlan;
  }

  public static QueryExecutionPlan create(
      String scratchpadSchemaName,
      ScrambleMetaSet scrambleMeta) {
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan();
    queryExecutionPlan.idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
    queryExecutionPlan.scrambleMeta = scrambleMeta;
    return queryExecutionPlan;
  }

  public static QueryExecutionPlan create(
      String scratchpadSchemaName,
      ScrambleMetaSet scrambleMeta,
      SelectQuery query) {
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan();
    queryExecutionPlan.idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
    queryExecutionPlan.scrambleMeta = scrambleMeta;
    queryExecutionPlan.root = createRootAndItsDependents(queryExecutionPlan.idCreator, query);
    return queryExecutionPlan;
  }

  public static QueryExecutionPlan create(
      String scratchpadSchemaName,
      ExecutableNodeBase root) {
    QueryExecutionPlan queryExecutionPlan = create(scratchpadSchemaName);
    queryExecutionPlan.root = root;
    return queryExecutionPlan;
  }
  
  static ExecutableNodeBase createRootAndItsDependents(IdCreator idCreator, SelectQuery query) {
    if (query.isSupportedAggregate()) {
      return createSelectAllExecutionNodeAndItsDependents(idCreator, query);
    } else {
      // Currently, the behaviour is the same.
      return createSelectAllExecutionNodeAndItsDependents(idCreator, query);
    }
  }
  
  static SelectAllExecutionNode createSelectAllExecutionNodeAndItsDependents(IdCreator idCreator, SelectQuery query) {
    SelectAllExecutionNode selectAll = new SelectAllExecutionNode(null);
    Pair<BaseTable, SubscriptionTicket> baseAndSubscriptionTicket = selectAll.createPlaceHolderTable("t");
    SelectQuery selectQuery = SelectQuery.create(new AsteriskColumn(), baseAndSubscriptionTicket.getLeft());
    selectQuery.addOrderby(query.getOrderby());
    if (query.getLimit().isPresent()) selectQuery.addLimit(query.getLimit().get());
    selectAll.setSelectQuery(selectQuery);

    if (query.isSupportedAggregate()) {
      AggExecutionNode dependent = createAggExecutionNodeAndItsDependents(idCreator, query);
      dependent.registerSubscriber(baseAndSubscriptionTicket.getRight());
//      selectAll.addDependency(dependent);
    }
    else {
      ProjectionNode dependent = createProjectionNodeAndItsDependents(idCreator, query);
      dependent.registerSubscriber(baseAndSubscriptionTicket.getRight());
//      selectAll.addDependency(dependent);
    }

    return selectAll;
  }
  
  static AggExecutionNode createAggExecutionNodeAndItsDependents(IdCreator idCreator, SelectQuery query) {
    AggExecutionNode node = new AggExecutionNode(idCreator, null);
    SubqueriesToDependentNodes.convertSubqueriesToDependentNodes(query, node);
    node.setSelectQuery(query);

    return node;
  }

  static ProjectionNode createProjectionNodeAndItsDependents(IdCreator idCreator, SelectQuery query) {
    ProjectionNode node = new ProjectionNode(idCreator, null);
    SubqueriesToDependentNodes.convertSubqueriesToDependentNodes(query, node);
    node.setSelectQuery(query);
    return node;
  }

}
