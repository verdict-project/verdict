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
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.exception.VerdictDBValueException;

public class QueryExecutionPlan implements ExecutablePlan, TempIdCreator {

  //  SelectQuery query;

  protected ScrambleMeta scrambleMeta;

  protected BaseQueryNode root;

  protected TempIdCreator idCreator;

  //  final int N_THREADS = 10;

  //  PostProcessor postProcessor;

  //  /**
  //   * 
  //   * @param queryString A select query
  //   * @throws UnexpectedTypeException 
  //   */
  //  public AggQueryExecutionPlan(DbmsConnection conn, SyntaxAbstract syntax, String queryString) throws VerdictDbException {
  //    this(conn, syntax, (SelectQueryOp) new NonValidatingSQLParser().toRelation(queryString));
  //  }

  public QueryExecutionPlan(String scratchpadSchemaName) {
    this.idCreator = new TempIdCreatorInScratchPadSchema(scratchpadSchemaName);
    this.scrambleMeta = new ScrambleMeta();
  }

  public QueryExecutionPlan(String scratchpadSchemaName, ScrambleMeta scrambleMeta) {
    this(scratchpadSchemaName);
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
      ScrambleMeta scrambleMeta,
      SelectQuery query) throws VerdictDBException {
    this(scratchpadSchemaName);
    setScrambleMeta(scrambleMeta);
    setSelectQuery(query);
  }
  
  public QueryExecutionPlan(String scratchpadSchemaName, BaseQueryNode root) {
    this(scratchpadSchemaName);
    this.root = root;
  }

  public int getSerialNumber() {
    return ((TempIdCreatorInScratchPadSchema) idCreator).getSerialNumber();
  }

  public ScrambleMeta getScrambleMeta() {
    return scrambleMeta;
  }

  public void setScrambleMeta(ScrambleMeta scrambleMeta) {
    this.scrambleMeta = scrambleMeta;
  }

  public void setSelectQuery(SelectQuery query) throws VerdictDBException {
    if (!query.isAggregateQuery()) {
      throw new VerdictDBTypeException(query);
    }
    this.root = makePlan(query);
  }

  public String getScratchpadSchemaName() {
    return ((TempIdCreatorInScratchPadSchema) idCreator).getScratchpadSchemaName();
  }

  public BaseQueryNode getRootNode() {
    return root;
  }

  public void setRootNode(BaseQueryNode root) {
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
  BaseQueryNode makePlan(SelectQuery query) throws VerdictDBException {
    BaseQueryNode root = SelectAllExecutionNode.create(idCreator, query);
    return root;
  }

  // clean up any intermediate materialized tables
  public void cleanUp() {
    ((TempIdCreatorInScratchPadSchema) idCreator).reset();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE)
        .append("root", root)
        .append("scrambleMeta", scrambleMeta)
        .toString();
  }

  public void compress() {
    List<BaseQueryNode> nodesToCompress = new ArrayList<>();
    // compress the node from bottom to up in order to replace the select query conveniently
    List<BaseQueryNode> traverse = new ArrayList<>();
    traverse.add(root);
    while (!traverse.isEmpty()) {
      BaseQueryNode node = traverse.get(0);
      traverse.remove(0);
      if (node.dependents.isEmpty() && !nodesToCompress.contains(node)) {
        nodesToCompress.add(node);
      }
      else traverse.addAll(node.dependents);
    }

    List<BaseQueryNode> history = new ArrayList<>();
    while (!nodesToCompress.isEmpty()) {
      BaseQueryNode node = nodesToCompress.get(0);
      nodesToCompress.remove(0);
      // Exception 1: has no parent(root), or has multiple parent
      // Exception 2: its parents has multiple dependents and this node share same queue with other dependents
      // Exception 3: two nodes are not SelectAllNode, ProjectionNode or AggregateNode
      boolean compressable = node.parents.size()==1 && !isSharingQueue(node);
      if (compressable) {
        BaseQueryNode parent = node.parents.get(0);
        if (((parent instanceof AggExecutionNode)||(parent instanceof SelectAllExecutionNode)||(parent instanceof ProjectionNode))
            && ((node instanceof AggExecutionNode)||(node instanceof SelectAllExecutionNode)||(node instanceof ProjectionNode)) ) {
          compressTwoNode(node, parent);
        }
      }
      history.add(node);
      for (BaseQueryNode parent:node.parents) {
        if (!history.contains(parent) && !nodesToCompress.contains(parent)) {
          nodesToCompress.add(parent);
        }
      }
    }
  }

  // Compress node and parent into parent, node will be useless
  void compressTwoNode(BaseQueryNode node, BaseQueryNode parent) {

    // Change the query of parents
    BaseTable placeholderTableinParent = ((QueryNodeWithPlaceHolders)parent).getPlaceholderTables().get(parent.dependents.indexOf(node));
    ((QueryNodeWithPlaceHolders)parent).getPlaceholderTables().remove(placeholderTableinParent);

    // If temp table is in from list of parent, just direct replace with the select query of node
    if (parent.selectQuery.getFromList().contains(placeholderTableinParent)) {
      int index = parent.selectQuery.getFromList().indexOf(placeholderTableinParent);
      node.selectQuery.setAliasName(parent.selectQuery.getFromList().get(index).getAliasName().get());
      parent.selectQuery.getFromList().set(index, node.selectQuery);
    }
    // Otherwise, it need to search filter to find the temp table
    else {
      List<SubqueryColumn> placeholderTablesinFilter = ((QueryNodeWithPlaceHolders)parent).getPlaceholderTablesinFilter();
      for (SubqueryColumn filter:placeholderTablesinFilter) {
        if (filter.getSubquery().getFromList().size()==1 && filter.getSubquery().getFromList().get(0).equals(placeholderTableinParent)) {
          filter.setSubquery(node.selectQuery);
        }
      }
    }

    // Compress the node tree
    parent.getListeningQueues().removeAll(node.broadcastingQueues);
    parent.getListeningQueues().addAll(node.getListeningQueues());
    parent.dependents.remove(node);
    parent.dependents.addAll(node.dependents);
    for (BaseQueryNode dependent:node.dependents) {
      dependent.parents.remove(node);
      dependent.parents.add(parent);
    }
  }

  // Return true if this node share queue with other dependant of its parent
  boolean isSharingQueue(BaseQueryNode node) {
    // must have one parent and this parent must have multiple dependents
    if (node.parents.size()!=1 || node.parents.get(0).dependents.size()<=1) {
      return false;
    }
    else {
      for (BaseQueryNode dependent:node.parents.get(0).dependents) {
        if (!dependent.equals(node) && node.getBroadcastingQueues().equals(dependent.getBroadcastingQueues())) {
          return true;
        }
      }
      return false;
    }
  }

  public BaseQueryNode getRoot() {
    return root;
  }


  public void setScalingNode() throws VerdictDBException{
    // Check from top to bottom to find AsyncAggExecutionNode
    List<BaseQueryNode> checkList = new ArrayList<>();
    List<AsyncAggExecutionNode> toScaleList = new ArrayList<>();
    checkList.add(root);
    List<BaseQueryNode> traversed = new ArrayList<>();
    while (!checkList.isEmpty()) {
      BaseQueryNode node = checkList.get(0);
      checkList.remove(0);
      traversed.add(node);
      if (node instanceof AsyncAggExecutionNode && !toScaleList.contains(node)) {
        toScaleList.add((AsyncAggExecutionNode) node);
      }
      for (BaseQueryNode dependent:node.dependents) {
        if (!traversed.contains(dependent)) {
          checkList.add(dependent);
        }
      }
    }
    for (AsyncAggExecutionNode asyncNode:toScaleList) {
      List<AggExecutionNode> aggNodeList = new ArrayList<>();
      aggNodeList.add((AggExecutionNode) asyncNode.getDependent(0));
      for (int i=1; i<asyncNode.getDependents().size(); i++) {
        aggNodeList.add((AggExecutionNode) asyncNode.getDependent(i).getDependent(1));
      }
      for (AggExecutionNode aggExecutionNode:aggNodeList) {
        AsyncAggScaleExecutionNode.create(idCreator, aggExecutionNode);
      }
    }
  }

  @Override
  public List<Integer> getNodeGroupIDs() {
    return Arrays.asList(0);
  }

  @Override
  public List<ExecutableNode> getNodesInGroup(int groupId) {
    List<ExecutableNode> nodes = new ArrayList<>();
    List<BaseQueryNode> pool = new LinkedList<>();
    pool.add(root);
    while (!pool.isEmpty()) {
      BaseQueryNode n = pool.remove(0);
      if (nodes.contains(n)) {
        continue;
      }
      nodes.add(n);
      pool.addAll(n.getDependents());
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
