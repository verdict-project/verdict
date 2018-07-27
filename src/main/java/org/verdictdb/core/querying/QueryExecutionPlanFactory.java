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

package org.verdictdb.core.querying;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.*;

import java.util.ArrayList;
import java.util.List;

public class QueryExecutionPlanFactory {

  /**
   * Creates a node tree and return it as an instance of QueryExecutionPlan.
   *
   * @param scratchpadSchemaName
   * @return
   */
  public static QueryExecutionPlan create(
      String scratchpadSchemaName) {
    resetUniqueIdGeneration();
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan();
    queryExecutionPlan.idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
    return queryExecutionPlan;
  }

  public static QueryExecutionPlan create(
      String scratchpadSchemaName,
      ScrambleMetaSet scrambleMeta) {
    resetUniqueIdGeneration();
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan();
    queryExecutionPlan.idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
    queryExecutionPlan.scrambleMeta = scrambleMeta;
    return queryExecutionPlan;
  }

  public static QueryExecutionPlan create(
      String scratchpadSchemaName,
      ScrambleMetaSet scrambleMeta,
      SelectQuery query) {
    resetUniqueIdGeneration();
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
    selectAll.setId(generateUniqueId());
    
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
    node.setId(generateUniqueId());
    convertSubqueriesToDependentNodes(query, node);
    node.setSelectQuery(query);
    return node;
  }

  static ProjectionNode createProjectionNodeAndItsDependents(IdCreator idCreator, SelectQuery query) {
    ProjectionNode node = new ProjectionNode(idCreator, null);
    node.setId(generateUniqueId());
    convertSubqueriesToDependentNodes(query, node);
    node.setSelectQuery(query);
    return node;
  }

  /**
   *
   * @param query A query that may include subqueries. The subqueries of this query will be replaced by
   * placeholders.
   * @param node
   */
  static void convertSubqueriesToDependentNodes(
      SelectQuery query,
      CreateTableAsSelectNode node) {
    IdCreator namer = node.getNamer();

    // from list
    for (AbstractRelation source : query.getFromList()) {
      int index = query.getFromList().indexOf(source);

      // If the table is subquery, we need to add it to dependency
      if (source instanceof SelectQuery) {
        CreateTableAsSelectNode dep;
        if (source.isSupportedAggregate()) {
          dep = createAggExecutionNodeAndItsDependents(namer, (SelectQuery) source);
        } else {
          dep = createProjectionNodeAndItsDependents(namer, (SelectQuery) source);
        }

        // use placeholders to mark the locations whose names will be updated in the future
        Pair<BaseTable, SubscriptionTicket> baseAndSubscriptionTicket = node.createPlaceHolderTable(source.getAliasName().get());
        query.getFromList().set(index, baseAndSubscriptionTicket.getLeft());
        dep.registerSubscriber(baseAndSubscriptionTicket.getRight());
      
      } else if (source instanceof JoinTable) {
        for (AbstractRelation s : ((JoinTable) source).getJoinList()) {
          int joinindex = ((JoinTable) source).getJoinList().indexOf(s);

          // If the table is subquery, we need to add it to dependency
          if (s instanceof SelectQuery) {
            CreateTableAsSelectNode dep;
            if (s.isSupportedAggregate()) {
              dep = createAggExecutionNodeAndItsDependents(namer, (SelectQuery) s);
            } else {
              dep = createProjectionNodeAndItsDependents(namer, (SelectQuery) s);
            }

            // use placeholders to mark the locations whose names will be updated in the future
            Pair<BaseTable, SubscriptionTicket> baseAndSubscriptionTicket = node.createPlaceHolderTable(s.getAliasName().get());
            ((JoinTable) source).getJoinList().set(joinindex, baseAndSubscriptionTicket.getLeft());
            dep.registerSubscriber(baseAndSubscriptionTicket.getRight());
          }
        }
      }
    }

    // Filter
    if (query.getFilter().isPresent()) {
      UnnamedColumn where = query.getFilter().get();
      List<UnnamedColumn> filters = new ArrayList<>();
      filters.add(where);
      while (!filters.isEmpty()) {
        UnnamedColumn filter = filters.get(0);
        filters.remove(0);

        // If filter is a subquery, we need to add it to dependency
        if (filter instanceof SubqueryColumn) {
          Pair<BaseTable, SubscriptionTicket> baseAndSubscriptionTicket;
          SubqueryColumn subqueryFilter = (SubqueryColumn) filter;
          if (subqueryFilter.getSubquery().getAliasName().isPresent()) {
            baseAndSubscriptionTicket =
                node.createPlaceHolderTable(subqueryFilter.getSubquery().getAliasName().get());
          } else {
            baseAndSubscriptionTicket = node.createPlaceHolderTable(namer.generateAliasName());
          }
          BaseTable base = baseAndSubscriptionTicket.getLeft();

          CreateTableAsSelectNode dep;
          SelectQuery subquery = subqueryFilter.getSubquery();
          if (subquery.isSupportedAggregate()) {    // TODO: this should check any aggregates
            dep = createAggExecutionNodeAndItsDependents(namer, subquery);
          } else {
            dep = createProjectionNodeAndItsDependents(namer, subquery);
          }
          dep.registerSubscriber(baseAndSubscriptionTicket.getRight());

          // To replace the subquery, we use the selectlist of the subquery and tempTable to
          // create a new non-aggregate subquery
          List<SelectItem> newSelectItem = new ArrayList<>();
          for (SelectItem item : subquery.getSelectList()) {
            if (item instanceof AliasedColumn) {
              newSelectItem.add(new AliasedColumn(
                  new BaseColumn(
                      base.getSchemaName(),
                      base.getAliasName().get(),
                      ((AliasedColumn) item).getAliasName()),
                  ((AliasedColumn) item).getAliasName()));
            } else if (item instanceof AsteriskColumn) {
              newSelectItem.add(new AsteriskColumn());
            }
          }
          SelectQuery newSubquery = SelectQuery.create(newSelectItem, base);
          if (((SubqueryColumn) filter).getSubquery().getAliasName().isPresent()) {
            newSubquery.setAliasName(((SubqueryColumn) filter).getSubquery().getAliasName().get());
          }
          ((SubqueryColumn) filter).setSubquery(newSubquery);
          // this doesn't seem to be necessary
          node.getPlaceholderTablesinFilter().add((SubqueryColumn) filter);
        } else if (filter instanceof ColumnOp) {
          filters.addAll(((ColumnOp) filter).getOperands());
        }
      }
    }

  }
  
  private static int nextIdentifier = 0;
  
  private static int generateUniqueId() {
    return nextIdentifier++;
  }
  
  private static void resetUniqueIdGeneration() {
    nextIdentifier = 0;
  }

}
