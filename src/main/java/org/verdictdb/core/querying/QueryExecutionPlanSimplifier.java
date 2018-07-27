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

import com.google.common.base.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.querying.ola.AsyncAggExecutionNode;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBValidationException;

import java.util.ArrayList;
import java.util.List;

public class QueryExecutionPlanSimplifier {
  
  /**
   * Simplifies the originalPlan in place.
   *
   * The parent node may consolidates with its child when all of the following conditions are
   * satisfied:
   * 1. The child node is a descendant of CreateTableAsSelectNode
   * 2. The child node is the unique source of the channel to which the child node is set to
   * broadcast
   * 3. The parent is the only subscriber of the child.
   *
   * @param originalPlan The plan to simplify
   * @throws VerdictDBValidationException This exception is thrown if the number of placeholders in
   * the parent does not match the number of the children.
   */
  public static void simplify2(QueryExecutionPlan originalPlan)
      throws VerdictDBValidationException {

    // Every iteration of this loop completely reconfigure placeholder list and subscription list
    // properly so that the next iteration does not need to know about the previous iteration.
    while (true) {
      ExecutableNodeBase parent = originalPlan.getRootNode();
      List<ExecutableNodeBase> sources = parent.getSources();
  
      // if the parent has no child, this loop will immediately finish.
      // if any child is consolidated, this loop will start all over again from the beginning
      // however, since each consolidation removes a node from the tree, this loop only iterates
      // as many times as the number of the nodes in the tree.
      boolean isConsolidated = false;
      for (int childIndex = 0; childIndex < sources.size(); childIndex++) {
        isConsolidated = consolidates(parent, childIndex);
        if (isConsolidated) {
          break;
        }
      }
      if (!isConsolidated) {
        break;
      }
    }
  }
  
  /**
   * Consolidates to a single child if the condition is met. This is a helper function for
   * simplify2().
   *
   * @param parent The parent node
   * @param childIndex The index of the child node to consolidate (if possible)
   * @return True if consolidated; false otherwise
   * @throws VerdictDBValidationException This exception is thrown if the number of placeholders in
   * the parent does not match the number of the children.
   */
  private static boolean consolidates(ExecutableNodeBase parent, int childIndex)
      throws VerdictDBValidationException {
  
    List<ExecutableNodeBase> sources = parent.getSources();
    ExecutableNodeBase child = sources.get(childIndex);
  
    // Check consolidation conditions
    
    // first condition: the child must inherits CreateTableAsSelectNode.
    if (!(child instanceof CreateTableAsSelectNode)) {
      return false;
    }
    
    // second condition: the child must be the unique broadcaster to the channel it broadcasts to.
    int channelSharingSourceCount = 0;
    int childChannel = parent.getChannelForSource(child);
    List<Pair<ExecutableNodeBase, Integer>> sourceAndChannelList = parent.getSourcesAndChannels();
    for (Pair<ExecutableNodeBase, Integer> sourceAndChannel : sourceAndChannelList) {
      int channel = sourceAndChannel.getRight();
      if (channel == childChannel) {
        channelSharingSourceCount += 1;
      }
    }
    if (channelSharingSourceCount > 1) {
      return false;
    }
    
    // third condition: the parent is the only subscriber of the child
    if (child.getSubscribers().size() > 1) {
      return false;
    }
    
    // Now actually consolidate
    
    // replace the placeholder associated with the child.
    // the existing placeholder is removed from the parent's placehoder table list.
    // the child itself may include one or more placeholders; so, those existing placeholders are
    // added into the parent's placeholder list.
    // The placeholders are associated with unique IDs; thus, safe to add/remove placehoders
    if (parent instanceof QueryNodeWithPlaceHolders && child instanceof QueryNodeBase) {
      // remove the placeholder for the child in the parent's list
      int channelForChild = parent.getChannelForSource(child);
      QueryNodeWithPlaceHolders placeholderParent = (QueryNodeWithPlaceHolders) parent;
      PlaceHolderRecord removedRecord =
          placeholderParent.removePlaceholderRecordForChannel(channelForChild);
      BaseTable baseTableToRemove = removedRecord.getPlaceholderTable();
      
      // replace the placeholder BaseTable (in the from list) with the SelectQuery of the child
      List<AbstractRelation> parentFromList = placeholderParent.getSelectQuery().getFromList();
      List<AbstractRelation> newParentFromList = new ArrayList<>();
      for (AbstractRelation originalSource : parentFromList) {
        AbstractRelation newSource = consolidateSource(originalSource, child, baseTableToRemove);
        newParentFromList.add(newSource);
      }
      placeholderParent.getSelectQuery().setFromList(newParentFromList);
      
      // Filter: replace the placeholder BaseTable (in the filter list) with
      // the SelectQuery of the child
      SelectQuery parentSelectQuery = placeholderParent.getSelectQuery();
      Optional<UnnamedColumn> parentFilterOptional = parentSelectQuery.getFilter();
      if (parentFilterOptional.isPresent()) {
        UnnamedColumn originalFilter = parentFilterOptional.get();
        UnnamedColumn newFilter = consolidateFilter(originalFilter, child, baseTableToRemove);
        parentSelectQuery.clearFilter();
        parentSelectQuery.addFilterByAnd(newFilter);
      }
      
      // add child's placeholders to the parent
      if (child instanceof QueryNodeWithPlaceHolders) {
        QueryNodeWithPlaceHolders placeHolderChild = (QueryNodeWithPlaceHolders) child;
        for (PlaceHolderRecord record : placeHolderChild.getPlaceholderRecords()) {
          placeholderParent.addPlaceholderRecord(record);
        }
      }
    }
    
    // the parent's subscription to the child is removed.
    // however, the parent now subscribes to the previous broadcastors to the child.
    parent.cancelSubscriptionTo(child);
    List<Pair<ExecutableNodeBase, Integer>> childSourceAndChannels = child.getSourcesAndChannels();
    for (Pair<ExecutableNodeBase, Integer> childSourceAndChannel : childSourceAndChannels) {
      ExecutableNodeBase childSource = childSourceAndChannel.getLeft();
      int childSourceChannel = childSourceAndChannel.getRight();
      child.cancelSubscriptionTo(childSource);
      parent.subscribeTo(childSource, childSourceChannel);
    }
    
    // One extra step: if the root node is the "select *" query without any group-by clauses
    // we just use the inner query.
    if (parent instanceof QueryNodeBase) {
      QueryNodeBase parentAsQueryNode = (QueryNodeBase) parent;
      SelectQuery parentSelectQuery = parentAsQueryNode.getSelectQuery();
      List<SelectItem> parentSelectList = parentSelectQuery.getSelectList();
      List<AbstractRelation> parentFromList = parentSelectQuery.getFromList();
      List<GroupingAttribute> parentGroupbyList = parentSelectQuery.getGroupby();
      if (parentSelectList.size() == 1
              && (parentSelectList.get(0) instanceof AsteriskColumn)
              && (parentGroupbyList.size() == 0)
              && (parentFromList.size() == 1)
              && (parentFromList.get(0) instanceof SelectQuery)) {
        SelectQuery innerQuery = (SelectQuery) parentFromList.get(0);
        innerQuery.clearAliasName();
        parentAsQueryNode.setSelectQuery(innerQuery);
      }
    }
    
    // indicates that a consolidation is performed
    return true;
  }
  
  /**
   * May consonlidate a single source with `child`. This is a helper function for simplify2().
   *
   * @param originalSource The original source
   * @param child The child node
   * @param baseTableToRemove The placeholder to be replaced
   * @return A new source
   */
  private static AbstractRelation consolidateSource(
      AbstractRelation originalSource, ExecutableNodeBase child, BaseTable baseTableToRemove) {
  
    // exception
    if (!(child instanceof QueryNodeBase)) {
      return originalSource;
    }
    
    SelectQuery childQuery = ((QueryNodeBase) child).getSelectQuery();
    
    if (originalSource instanceof BaseTable) {
      BaseTable baseTableSource = (BaseTable) originalSource;
      if (baseTableSource.equals(baseTableToRemove)) {
        childQuery.setAliasName(baseTableToRemove.getAliasName().get());
        return childQuery;
      } else {
        return originalSource;
      }
    } else if (originalSource instanceof JoinTable) {
      JoinTable joinTableSource = (JoinTable) originalSource;
      List<AbstractRelation> joinSourceList = joinTableSource.getJoinList();
      List<AbstractRelation> newJoinSourceList = new ArrayList<>();
      for (AbstractRelation joinSource : joinSourceList) {
        newJoinSourceList.add(consolidateSource(joinSource, child, baseTableToRemove));
      }
      joinTableSource.setJoinList(newJoinSourceList);
      return joinTableSource;
    } else {
      return originalSource;
    }
  }
  
  /**
   * May consolidate a single filter with `child`. This is a helper function for simplify2().
   *
   * @param originalFilter The original filter
   * @param child The child node
   * @param baseTableToRemove The placeholder to be replaced
   * @return
   */
  private static UnnamedColumn consolidateFilter(
      UnnamedColumn originalFilter, ExecutableNodeBase child, BaseTable baseTableToRemove) {
    
    // exception
    if (!(child instanceof QueryNodeBase)) {
      return originalFilter;
    }
    
    SelectQuery childSelectQuery = ((QueryNodeBase) child).getSelectQuery();
  
    if (originalFilter instanceof ColumnOp) {
      ColumnOp originalColumnOp = (ColumnOp) originalFilter;
      List<UnnamedColumn> newOperands = new ArrayList<>();
      for (UnnamedColumn o : originalColumnOp.getOperands()) {
        newOperands.add(consolidateFilter(o, child, baseTableToRemove));
      }
      ColumnOp newColumnOp = new ColumnOp(originalColumnOp.getOpType(), newOperands);
      return newColumnOp;
    } else if (originalFilter instanceof SubqueryColumn) {
      SubqueryColumn originalSubquery = (SubqueryColumn) originalFilter;
      SelectQuery subquery = originalSubquery.getSubquery();
      List<AbstractRelation> subqueryFromList = subquery.getFromList();
      if (subqueryFromList.size() == 1 && subqueryFromList.get(0).equals(baseTableToRemove)) {
        originalSubquery.setSubquery(childSelectQuery);
      }
      return originalSubquery;
    }
    
    return originalFilter;
  }
  

  /**
   * @param originalPlan
   * @return a deepcopy of plan which is simplified
   */
  public static QueryExecutionPlan simplify(QueryExecutionPlan originalPlan) {
    // Deep copy originalPlan
    QueryExecutionPlan plan = originalPlan.deepcopy();

    List<ExecutableNodeBase> nodesToCompress = new ArrayList<>();
    // compress the node from bottom to up in order to replace the select query conveniently
    List<ExecutableNodeBase> traverse = new ArrayList<>();
    traverse.add(plan.getRoot());
    while (!traverse.isEmpty()) {
      ExecutableNodeBase node = traverse.get(0);
      traverse.remove(0);
      if (node.getDependentNodeCount() == 0 && !nodesToCompress.contains(node)) {
        nodesToCompress.add(node);
      } else traverse.addAll(node.getExecutableNodeBaseDependents());
    }

    List<ExecutableNodeBase> history = new ArrayList<>();
    while (!nodesToCompress.isEmpty()) {
      ExecutableNodeBase node = nodesToCompress.remove(0);
      List<ExecutableNodeBase> nodeParentsSaved =
          new ArrayList<>(node.getExecutableNodeBaseParents());

      // Exception 1: has no parent(root), or has multiple parent
      // Exception 2: its parents has multiple dependents and this node share same queue with other
      // dependents
      // Exception 3: two nodes are not SelectAllNode, ProjectionNode or AggregateNode
      boolean compressable =
          node.getExecutableNodeBaseParents().size() == 1 && !isSharingQueue(node);
      if (compressable) {
        ExecutableNodeBase parent = node.getExecutableNodeBaseParents().get(0);
        if (((parent instanceof AggExecutionNode)
                || (parent instanceof SelectAllExecutionNode)
                || (parent instanceof ProjectionNode && !(parent instanceof AsyncAggExecutionNode)))
            && ((node instanceof AggExecutionNode)
                || (node instanceof SelectAllExecutionNode)
                || (node instanceof ProjectionNode && !(node instanceof AsyncAggExecutionNode)))) {
          compressTwoNode(node, parent);
        }
      }
      history.add(node);

      // the parent information of the "node" has been removed
      for (ExecutableNodeBase parent : nodeParentsSaved) {
        if (!history.contains(parent) && !nodesToCompress.contains(parent)) {
          nodesToCompress.add(parent);
        }
      }
    }
    return plan;
  }

  // Compress node and parent into parent, node will be useless
  static void compressTwoNode(ExecutableNodeBase node, ExecutableNodeBase parent) {
    if (!(node instanceof QueryNodeBase) || !(parent instanceof QueryNodeBase)) {
      return;
    }
    QueryNodeBase parentQuery = (QueryNodeBase) parent;
    QueryNodeBase nodeQuery = (QueryNodeBase) node;

    // Change the query of parents
    BaseTable placeholderTableinParent =
        ((QueryNodeWithPlaceHolders) parent)
            .getPlaceholderTables()
            .get(parent.getExecutableNodeBaseDependents().indexOf(node));
    ((QueryNodeWithPlaceHolders) parent).getPlaceholderTables().remove(placeholderTableinParent);

    // If temp table is in from list of parent, just direct replace with the select query of node
    boolean find = false;
    for (AbstractRelation table : parentQuery.getSelectQuery().getFromList()) {
      if (table instanceof BaseTable && table.equals(placeholderTableinParent)) {
        int index = parentQuery.getSelectQuery().getFromList().indexOf(table);
        nodeQuery
            .getSelectQuery()
            .setAliasName(
                parentQuery.getSelectQuery().getFromList().get(index).getAliasName().get());
        parentQuery.getSelectQuery().getFromList().set(index, nodeQuery.getSelectQuery());
        find = true;
        break;
      } else if (table instanceof JoinTable) {
        for (AbstractRelation joinTable : ((JoinTable) table).getJoinList()) {
          if (joinTable instanceof BaseTable && joinTable.equals(placeholderTableinParent)) {
            int index = ((JoinTable) table).getJoinList().indexOf(joinTable);
            nodeQuery.getSelectQuery().setAliasName(joinTable.getAliasName().get());
            ((JoinTable) table).getJoinList().set(index, nodeQuery.getSelectQuery());
            find = true;
            break;
          }
        }
        if (find) break;
      }
    }

    // Otherwise, it need to search filter to find the temp table
    if (!find) {
      List<SubqueryColumn> placeholderTablesinFilter =
          ((QueryNodeWithPlaceHolders) parent).getPlaceholderTablesinFilter();
      for (SubqueryColumn filter : placeholderTablesinFilter) {
        if (filter.getSubquery().getFromList().size() == 1
            && filter.getSubquery().getFromList().get(0).equals(placeholderTableinParent)) {
          filter.setSubquery(nodeQuery.getSelectQuery());
        }
      }
    }

    // Move node's placeholderTable to parent's
    ((QueryNodeWithPlaceHolders) parent)
        .getPlaceholderTables().addAll(((QueryNodeWithPlaceHolders) node).getPlaceholderTables());

    // Compress the node tree
    parentQuery.cancelSubscriptionTo(nodeQuery);
    for (Pair<ExecutableNodeBase, Integer> s : nodeQuery.getSourcesAndChannels()) {
      parentQuery.subscribeTo(s.getLeft(), s.getRight());
    }
    //    parent.getListeningQueues().removeAll(node.broadcastingQueues);
    //    parent.getListeningQueues().addAll(node.getListeningQueues());
    //    parent.dependents.remove(node);
    //    parent.dependents.addAll(node.dependents);
    //    for (BaseQueryNode dependent:node.dependents) {
    //      dependent.parents.remove(node);
    //      dependent.parents.add(parent);
    //    }
  }

  /**
   *
   * @param node
   * @return true if node is sharing channel with other sources of its subsriber
   */
   static boolean isSharingQueue(ExecutableNodeBase node) {
    // must have one parent and this parent must have multiple dependents
    if (node.getExecutableNodeBaseParents().size() != 1
        || node.getExecutableNodeBaseParents().get(0).getDependentNodeCount() <= 1) {
      return false;
    }
    else {
      ExecutableNodeBase parent = node.getExecutableNodeBaseParents().get(0);
      int nodeIndex = parent.getSources().indexOf(node);
      for (ExecutableNodeBase dependent : parent.getExecutableNodeBaseDependents()) {
        if (!dependent.equals(node)) {
          int dependentIndex = parent.getSources().indexOf(dependent);
          if (parent.getSourcesAndChannels().get(nodeIndex).getRight().equals(
              parent.getSourcesAndChannels().get(dependentIndex).getRight())) {
            return true;
          }
        }
      }
      return false;
    }
  }
}
