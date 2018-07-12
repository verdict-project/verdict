/*
 * Copyright 2018 University of Michigan
 * 
 * You must contact Barzan Mozafari (mozafari@umich.edu) or Yongjoo Park (pyongjoo@umich.edu) to discuss
 * how you could use, modify, or distribute this code. By default, this code is not open-sourced and we do
 * not license this code.
 */

package org.verdictdb.core.querying;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.execution.ExecutableNode;
import org.verdictdb.core.execution.ExecutablePlan;
import org.verdictdb.core.querying.ola.AsyncAggExecutionNode;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.exception.VerdictDBValueException;

public class QueryExecutionPlan implements ExecutablePlan, IdCreator {
  
  protected ScrambleMetaSet scrambleMeta;

  protected ExecutableNodeBase root;

  protected IdCreator idCreator;

  public QueryExecutionPlan(String scratchpadSchemaName) {
    this.scrambleMeta = new ScrambleMetaSet();
    this.idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
  }

  public QueryExecutionPlan(String scratchpadSchemaName, ScrambleMetaSet scrambleMeta) {
    this.idCreator = new TempIdCreatorInScratchpadSchema(scratchpadSchemaName);
    this.scrambleMeta = scrambleMeta;
  }

  /**
   * 
   * @param query  A well-formed select query object
   * @throws VerdictDBValueException 
   * @throws VerdictDBException 
   */
  public QueryExecutionPlan(
      String scratchpadSchemaName,
      ScrambleMetaSet scrambleMeta,
      SelectQuery query) throws VerdictDBException {
    
    this(scratchpadSchemaName);
    setScrambleMeta(scrambleMeta);
    setSelectQuery(query);
  }
  
  public QueryExecutionPlan(String scratchpadSchemaName, ExecutableNodeBase root) {
    this(scratchpadSchemaName);
    this.root = root;
  }

  public int getSerialNumber() {
    return ((TempIdCreatorInScratchpadSchema) idCreator).getSerialNumber();
  }

  public ScrambleMetaSet getScrambleMeta() {
    return scrambleMeta;
  }

  public void setScrambleMeta(ScrambleMetaSet scrambleMeta) {
    this.scrambleMeta = scrambleMeta;
  }

  public void setSelectQuery(SelectQuery query) throws VerdictDBException {
    // TODO: this should also test if subqueries include aggregates
    // Change this to something like 'doesContainSupportedAggregateInDescendents()'.
    if (!query.isSupportedAggregate()) {
      throw new VerdictDBTypeException(query);
    }
    this.root = makePlan(query);
  }

  public String getScratchpadSchemaName() {
    return ((TempIdCreatorInScratchpadSchema) idCreator).getScratchpadSchemaName();
  }

  public ExecutableNodeBase getRootNode() {
    return root;
  }

  public void setRootNode(ExecutableNodeBase root) {
    this.root = root;
  }

  /** 
   * Creates a tree in which each node is QueryExecutionNode. Each AggQueryExecutionNode corresponds to
   * an aggregate query, whether it is the main query or a subquery.
   * 
   * 1. Each QueryExecutionNode is supposed to run on a separate thread.
   * 2. Restrict the aggregate subqueries to appear in the where clause or in the from clause 
   *    (i.e., not in the select list, not in having or group-by)
   * 3. Each node cannot include any correlated predicate (i.e., the column that appears in outer queries).
   *   (1) In the future, we should convert a correlated subquery into a joined subquery (if possible).
   *   (2) Otherwise, the entire query including a correlated subquery must be the query of a single node.
   * 4. The results of AggNode and ProjectionNode are stored as a materialized view; the names of those
   *    materialized views are passed to their parents for potential additional processing or reporting.
   * 
   * //@param conn
   * @param query
   * @return Pair of roots of the tree and post-processing interface.
   * @throws VerdictDBValueException 
   * @throws VerdictDBTypeException 
   */
  ExecutableNodeBase makePlan(SelectQuery query) throws VerdictDBException {
    ExecutableNodeBase root = SelectAllExecutionNode.create(idCreator, query);
    return root;
  }

  // clean up any intermediate materialized tables
  public void cleanUp() {
    ((TempIdCreatorInScratchpadSchema) idCreator).reset();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE)
        .append("root", root)
        .append("scrambleMeta", scrambleMeta)
        .toString();
  }

  // TODO: Move this to another class: QueryExecutionPlanSimplifier
  public void compress() {
    List<ExecutableNodeBase> nodesToCompress = new ArrayList<>();
    // compress the node from bottom to up in order to replace the select query conveniently
    List<ExecutableNodeBase> traverse = new ArrayList<>();
    traverse.add(root);
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
//<<<<<<< HEAD
//        ExecutableNodeBase parent = node.getExecutableNodeBaseParents().get(0);
//        if (((parent instanceof AggExecutionNode)||(parent instanceof SelectAllExecutionNode)||(parent instanceof ProjectionNode))
//            && ((node instanceof AggExecutionNode)||(node instanceof SelectAllExecutionNode)||(node instanceof ProjectionNode)) ) {
//=======
        ExecutableNodeBase parent = node.getExecutableNodeBaseParents().get(0);
        if (((parent instanceof AggExecutionNode) || (parent instanceof SelectAllExecutionNode) || 
             (parent instanceof ProjectionNode && !(parent instanceof AsyncAggExecutionNode)))
          && ((node instanceof AggExecutionNode) ||(node instanceof SelectAllExecutionNode) || 
              (node instanceof ProjectionNode && !(node instanceof AsyncAggExecutionNode))) ) {
//>>>>>>> origin/joezhong-scale
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
  }

  // Compress node and parent into parent, node will be useless
  void compressTwoNode(ExecutableNodeBase node, ExecutableNodeBase parent) {
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
  boolean isSharingQueue(ExecutableNodeBase node) {
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

  public ExecutableNodeBase getRoot() {
    return root;
  }
  
  @Override
  public List<Integer> getNodeGroupIDs() {
    return Arrays.asList(0);
  }

  @Override
  public List<ExecutableNode> getNodesInGroup(int groupId) {
    List<ExecutableNode> nodes = new ArrayList<>();
    List<ExecutableNodeBase> pool = new LinkedList<>();
    pool.add(root);
    while (!pool.isEmpty()) {
      ExecutableNodeBase n = pool.remove(0);
      if (nodes.contains(n)) {
        continue;
      }
      nodes.add(n);
      pool.addAll(n.getExecutableNodeBaseDependents());
    }
    return nodes;
  }

  @Override
  public ExecutableNode getReportingNode() {
    return root;
  }

  @Override
  public String generateAliasName() {
    return idCreator.generateAliasName();
  }

  @Override
  public Pair<String, String> generateTempTableName() {
    return idCreator.generateTempTableName();
  }
}
