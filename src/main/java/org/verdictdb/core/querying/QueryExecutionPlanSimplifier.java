package org.verdictdb.core.querying;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.execution.ExecutableNode;
import org.verdictdb.core.execution.ExecutablePlan;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.querying.ola.AsyncAggExecutionNode;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

import java.util.*;

public class QueryExecutionPlanSimplifier {

  /**
   *
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
      }
      else traverse.addAll(node.getExecutableNodeBaseDependents());
    }

    List<ExecutableNodeBase> history = new ArrayList<>();
    while (!nodesToCompress.isEmpty()) {
      ExecutableNodeBase node = nodesToCompress.get(0);
      nodesToCompress.remove(0);
      // Exception 1: has no parent(root), or has multiple parent
      // Exception 2: its parents has multiple dependents and this node share same queue with other dependents
      // Exception 3: two nodes are not SelectAllNode, ProjectionNode or AggregateNode
      boolean compressable = node.getExecutableNodeBaseParents().size() == 1 && !isSharingQueue(node);
      if (compressable) {
        ExecutableNodeBase parent = node.getExecutableNodeBaseParents().get(0);
        if (((parent instanceof AggExecutionNode) || (parent instanceof SelectAllExecutionNode) ||
            (parent instanceof ProjectionNode && !(parent instanceof AsyncAggExecutionNode)))
            && ((node instanceof AggExecutionNode) ||(node instanceof SelectAllExecutionNode) ||
            (node instanceof ProjectionNode && !(node instanceof AsyncAggExecutionNode))) ) {
          compressTwoNode(node, parent);
        }
      }
      history.add(node);
      for (ExecutableNodeBase parent : node.getExecutableNodeBaseParents()) {
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
    BaseTable placeholderTableinParent = ((QueryNodeWithPlaceHolders)parent).getPlaceholderTables().get(parent.getExecutableNodeBaseDependents().indexOf(node));
    ((QueryNodeWithPlaceHolders)parent).getPlaceholderTables().remove(placeholderTableinParent);

    // If temp table is in from list of parent, just direct replace with the select query of node
    if (parentQuery.getSelectQuery().getFromList().contains(placeholderTableinParent)) {
      int index = parentQuery.getSelectQuery().getFromList().indexOf(placeholderTableinParent);
      nodeQuery.getSelectQuery().setAliasName(
          parentQuery.getSelectQuery().getFromList().get(index).getAliasName().get());
      parentQuery.getSelectQuery().getFromList().set(index, nodeQuery.getSelectQuery());
    }
    // Otherwise, it need to search filter to find the temp table
    else {
      List<SubqueryColumn> placeholderTablesinFilter = ((QueryNodeWithPlaceHolders)parent).getPlaceholderTablesinFilter();
      for (SubqueryColumn filter:placeholderTablesinFilter) {
        if (filter.getSubquery().getFromList().size()==1 && filter.getSubquery().getFromList().get(0).equals(placeholderTableinParent)) {
          filter.setSubquery(nodeQuery.getSelectQuery());
        }
      }
    }

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

  // Return true if this node share queue with other dependant of its parent
  static boolean isSharingQueue(ExecutableNodeBase node) {
    // must have one parent and this parent must have multiple dependents
    if (node.getExecutableNodeBaseParents().size() != 1 ||
        node.getExecutableNodeBaseParents().get(0).getDependentNodeCount() <= 1) {
      return false;
    }
    else {
      for (ExecutableNodeBase dependent : node.getExecutableNodeBaseParents().get(0).getExecutableNodeBaseDependents()) {
        if (!dependent.equals(node)) {
          for (ExecutableNode s1 : node.getSubscribers()) {
            for (ExecutableNode s2 : dependent.getSubscribers()) {
              if (s1.equals(s2)) {
                return true;
              }
            }
          }
//          && node.getBroadcastingQueues().equals(dependent.getBroadcastingQueues())) {
//        }
//          return true;
        }
      }
      return false;
    }
  }

}
