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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.coordinator.QueryContext;
import org.verdictdb.core.querying.ola.AsyncAggExecutionNode;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasReference;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.GroupingAttribute;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.OrderbyAttribute;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.core.sqlobject.UnnamedColumn;

public class QueryExecutionPlanFactory {

  /**
   * Creates a node tree and return it as an instance of QueryExecutionPlan.
   *
   * @param scratchpadSchemaName
   * @return
   */
  public static QueryExecutionPlan create(String scratchpadSchemaName) {
    resetUniqueIdGeneration();
    IdCreator idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan(idCreator);
    return queryExecutionPlan;
  }

  public static QueryExecutionPlan create(
      String scratchpadSchemaName, ScrambleMetaSet scrambleMeta) {
    resetUniqueIdGeneration();
    IdCreator idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan(idCreator);
    queryExecutionPlan.scrambleMeta = scrambleMeta;
    return queryExecutionPlan;
  }

  public static QueryExecutionPlan create(
      String scratchpadSchemaName, ScrambleMetaSet scrambleMeta, SelectQuery query) {
    resetUniqueIdGeneration();
    IdCreator idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan(idCreator);
    queryExecutionPlan.scrambleMeta = scrambleMeta;
    queryExecutionPlan.root = createRootAndItsDependents(queryExecutionPlan.idCreator, query);
    return queryExecutionPlan;
  }

  public static QueryExecutionPlan create(
      String scratchpadSchemaName,
      ScrambleMetaSet scrambleMeta,
      SelectQuery query,
      QueryContext context) {
    resetUniqueIdGeneration();
    IdCreator idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName, context);
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan(idCreator);
    queryExecutionPlan.scrambleMeta = scrambleMeta;
    queryExecutionPlan.root = createRootAndItsDependents(queryExecutionPlan.idCreator, query);
    return queryExecutionPlan;
  }

  public static QueryExecutionPlan create(String scratchpadSchemaName, ExecutableNodeBase root) {
    QueryExecutionPlan queryExecutionPlan = create(scratchpadSchemaName);
    queryExecutionPlan.root = root;
    return queryExecutionPlan;
  }

  private static ExecutableNodeBase createRootAndItsDependents(
      IdCreator idCreator, SelectQuery query) {
    if (query.isSupportedAggregate()) {
      return createSelectAllExecutionNodeAndItsDependents(idCreator, query);
    } else {
      // Currently, the behaviour is the same.
      return createSelectAllExecutionNodeAndItsDependents(idCreator, query);
    }
  }

  private static SelectAllExecutionNode createSelectAllExecutionNodeAndItsDependents(
      IdCreator idCreator, SelectQuery query) {
    SelectAllExecutionNode selectAll = new SelectAllExecutionNode(idCreator, null);

    Pair<BaseTable, SubscriptionTicket> baseAndSubscriptionTicket =
        selectAll.createPlaceHolderTable("t");
    SelectQuery selectQuery =
        SelectQuery.create(new AsteriskColumn(), baseAndSubscriptionTicket.getLeft());
    //    selectQuery.addOrderby(query.getOrderby());
    //    if (query.getLimit().isPresent()) selectQuery.addLimit(query.getLimit().get());
    selectAll.setSelectQuery(selectQuery);

    if (query.isSupportedAggregate()) {
      AggExecutionNode dependent = createAggExecutionNodeAndItsDependents(idCreator, query);
      dependent.registerSubscriber(baseAndSubscriptionTicket.getRight());
      //      selectAll.addDependency(dependent);
    } else {
      ProjectionNode dependent = createProjectionNodeAndItsDependents(idCreator, query);
      dependent.registerSubscriber(baseAndSubscriptionTicket.getRight());
      //      selectAll.addDependency(dependent);
    }

    return selectAll;
  }

  private static AggExecutionNode createAggExecutionNodeAndItsDependents(
      IdCreator idCreator, SelectQuery query) {
    AggExecutionNode node = new AggExecutionNode(idCreator, null);
    convertSubqueriesToDependentNodes(query, node);
    //    query.addOrderby(query.getOrderby());
    //  if (query.getLimit().isPresent()) selectQuery.addLimit(query.getLimit().get());

    /*
     * Here we add expressions in HAVING, GROUP-BY and ORDER-BY into the select list of
     * the original query to calculate their approximate values later.
     *
     * We have two assumptions:
     * 1. If the having clause includes a base column outside an aggregate function,
     *    the column must have appeared in the groupby clause. Otherwise, the query itself is
     *    ill-formed.
     * 2. If the having clause includes an aggregate function, the function can safely appear
     *    in the select list because the select list must already include other aggregate functions.
     *    Otherwise, it is not an aggregate query;thus, the having clause should not have been
     *    used in the first place.
     *
     * For example, the following original query:
     * SELECT a
     * FROM t
     * GROUP BY g
     * HAVING h
     * ORDER BY o
     *
     * becomes:
     * SELECT a, g, h, o
     * FROM t
     * GROUP BY g
     * HAVING h
     * ORDER BY o
     *
     * so that with AsyncAggExecutionNode, we can do something like below:
     * (note that this is a lot more simpler than the actual re-written query)
     *
     * SELECT a
     * FROM (SELECT approx(a) as a, approx(g) as g, approx(h) as h, approx(o) as o
     *       FROM scrambled(t))
     * GROUP BY g
     * HAVING h
     * ORDER BY o
     *
     * The logic in AsyncQueryExecutionPlan and AsyncAggExecutionNode will generate
     * {approx(g), approx(h), ...} for us and we simply need to substitute these expressions
     * accordingly later.
     *
     */

    int groupbyCount = 0;
    for (GroupingAttribute attr : query.getGroupby()) {
      UnnamedColumn groupByCol = findActualGroupByExpression(attr, query);
      query.addSelectItem(
          new AliasedColumn(
              groupByCol, AsyncAggExecutionNode.getGroupByAlias() + (groupbyCount++)));
    }

    if (query.getHaving().isPresent()) {
      UnnamedColumn havingCopy = query.getHaving().get().deepcopy();
      AliasedColumn havingColumn =
          new AliasedColumn(havingCopy, AsyncAggExecutionNode.getHavingConditionAlias());
      query.addSelectItem(havingColumn);
    }

    int orderByCount = 0;
    for (OrderbyAttribute attribute : query.getOrderby()) {
      GroupingAttribute attr = attribute.getAttribute();
      UnnamedColumn orderByCol = findActualGroupByExpression(attr, query);
      query.addSelectItem(
          new AliasedColumn(
              orderByCol, AsyncAggExecutionNode.getOrderByAlias() + (orderByCount++)));
    }

    node.setSelectQuery(query);
    return node;
  }

  private static UnnamedColumn findActualGroupByExpression(
      GroupingAttribute attr, SelectQuery query) {
    UnnamedColumn col = (UnnamedColumn) attr;
    if (attr instanceof AliasReference) {
      AliasReference ref = (AliasReference) attr;
      String origAliasName =
          ref.getAliasName().replaceAll("\"", "").replaceAll("'", "").replaceAll("`", "");
      for (SelectItem item : query.getSelectList()) {
        if (item instanceof AliasedColumn) {
          AliasedColumn ac = (AliasedColumn) item;
          String otherAliasName =
              ac.getAliasName().replaceAll("\"", "").replaceAll("'", "").replaceAll("`", "");
          if (origAliasName.equals(otherAliasName)) {
            col = ac.getColumn();
            break;
          }
        }
      }
    }
    return col;
  }

  private static ProjectionNode createProjectionNodeAndItsDependents(
      IdCreator idCreator, SelectQuery query) {
    ProjectionNode node = new ProjectionNode(idCreator, null);
    convertSubqueriesToDependentNodes(query, node);
    node.setSelectQuery(query);
    return node;
  }

  /**
   * @param query A query that may include subqueries. The subqueries of this query will be replaced
   *     by placeholders.
   * @param node
   */
  private static void convertSubqueriesToDependentNodes(
      SelectQuery query, CreateTableAsSelectNode node) {
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
        Pair<BaseTable, SubscriptionTicket> baseAndSubscriptionTicket =
            node.createPlaceHolderTable(source.getAliasName().get());
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
            Pair<BaseTable, SubscriptionTicket> baseAndSubscriptionTicket =
                node.createPlaceHolderTable(s.getAliasName().get());
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
          if (subquery.isSupportedAggregate()) { // TODO: this should check any aggregates
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
              newSelectItem.add(
                  new AliasedColumn(
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
