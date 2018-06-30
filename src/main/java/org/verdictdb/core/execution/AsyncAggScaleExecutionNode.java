package org.verdictdb.core.execution;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.execution.ola.AsyncAggExecutionNode;
import org.verdictdb.core.execution.ola.Dimension;
import org.verdictdb.core.execution.ola.HyperTableCube;
import org.verdictdb.core.query.*;
import org.verdictdb.core.ScrambleMeta;
import org.verdictdb.exception.VerdictDBException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AsyncAggScaleExecutionNode extends ProjectionExecutionNode {

  // Default value. Will be modified when executeNode() is called.
  double scaleFactor = 1.0;
  List<ColumnOp> aggColumnlist = new ArrayList<>();

  protected AsyncAggScaleExecutionNode(QueryExecutionPlan plan) {
    super(plan);
  }

  public static AsyncAggScaleExecutionNode create(QueryExecutionPlan plan, AggExecutionNode aggNode) throws VerdictDBException {
    AsyncAggScaleExecutionNode node = new AsyncAggScaleExecutionNode(plan);

    // Setup select list
    Pair<BaseTable, ExecutionTokenQueue> baseAndQueue = node.createPlaceHolderTable("to_scale_query");
    List<SelectItem> newSelectList = aggNode.getSelectQuery().deepcopy().getSelectList();
    for (SelectItem selectItem:newSelectList) {
      // invariant: the agg column must be aliased column
      if (selectItem instanceof AliasedColumn) {
        int index = newSelectList.indexOf(selectItem);
        UnnamedColumn col = ((AliasedColumn) selectItem).getColumn();
        if (AsyncAggScaleExecutionNode.isAggregateColumn(col)) {
          ColumnOp aggColumn = new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
              ConstantColumn.valueOf(node.scaleFactor), new BaseColumn("to_scale_query" ,((AliasedColumn) selectItem).getAliasName())
          ));
          node.aggColumnlist.add(aggColumn);
          newSelectList.set(index, new AliasedColumn(aggColumn, ((AliasedColumn) selectItem).getAliasName()));
        }
        else {
          newSelectList.set(index, new AliasedColumn(new BaseColumn("to_scale_query" ,((AliasedColumn) selectItem).getAliasName()),
              ((AliasedColumn) selectItem).getAliasName()));
        }
      }
    }
    // Setup from table


    SelectQuery query = SelectQuery.create(newSelectList, baseAndQueue.getLeft());
    node.setSelectQuery(query);

    // Set this node to broadcast to the parents of asyncNode
    // Also remove the dependency
    for (QueryExecutionNode parent:aggNode.getParents()) {
      int index = parent.dependents.indexOf(aggNode);
      ExecutionTokenQueue queue = new ExecutionTokenQueue();
      // If parent is AsyncAggExecution, all dependents share a listening queue
      if (parent instanceof AsyncAggExecutionNode) {
        node.addBroadcastingQueue(parent.getListeningQueue(0));
      }
      else {
        parent.getListeningQueues().set(index, queue);
        node.addBroadcastingQueue(queue);
      }
      parent.dependents.set(index, node);
      node.addParent(parent);
    }

    // Set the asyncNode only to broadcast to this node
    // Also set parent
    aggNode.getBroadcastingQueues().clear();
    aggNode.addBroadcastingQueue(baseAndQueue.getRight());
    aggNode.getParents().clear();
    node.addDependency(aggNode);

    return node;
  }

  // Currently, only need to judge whether it is sum or count
  // Also replace alias name
  public static boolean isAggregateColumn(UnnamedColumn sel) {
    List<SelectItem> itemToCheck = new ArrayList<>();
    itemToCheck.add(sel);
    while (!itemToCheck.isEmpty()) {
      SelectItem s = itemToCheck.get(0);
      itemToCheck.remove(0);
      if (s instanceof ColumnOp) {
        if (((ColumnOp) s).getOpType().equals("count") || ((ColumnOp) s).getOpType().equals("sum")) {
          return true;
        }
        else itemToCheck.addAll(((ColumnOp) s).getOperands());
      }
    }
    return false;
  }

  @Override
  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults)
      throws VerdictDBException {


    for (ExecutionInfoToken downstreamResult:downstreamResults) {
      List<HyperTableCube> cubes = (List<HyperTableCube>) downstreamResult.getValue("hyperTableCube");
      if (cubes != null) {
        // Calculate the scale factor
        scaleFactor = calculateScaleFactor(cubes);
        // Substitute the scale factor
        for (ColumnOp col : aggColumnlist) {
          col.setOperand(0, ConstantColumn.valueOf(scaleFactor));
        }
      }
    }
    ExecutionInfoToken token = super.executeNode(conn, downstreamResults);
    return token;
  }

  @Override
  public QueryExecutionNode deepcopy() {
    AsyncAggScaleExecutionNode node = new AsyncAggScaleExecutionNode(plan);
    copyFields(this, node);
    node.scaleFactor = scaleFactor;
    node.aggColumnlist = aggColumnlist;
    return node;
  }

  // Currently, assume block size is uniform
  public double calculateScaleFactor(List<HyperTableCube> cubes) {
    AsyncAggExecutionNode asyncNode;
    if (this.getParents().size()==2) {
      asyncNode = (AsyncAggExecutionNode)this.getParents().get(1);
    }
    else {
      asyncNode = this.getParents().get(0).getParents().size() == 2?
          (AsyncAggExecutionNode) this.getParents().get(0).getParents().get(1):(AsyncAggExecutionNode) this.getParents().get(0).getParents().get(0);
    }
    ScrambleMeta scrambleMeta = asyncNode.getScrambleMeta();
    int totalSize = 1;
    for (Dimension d:cubes.get(0).getDimensions()) {
      int blockCount = scrambleMeta.getAggregationBlockCount(d.getSchemaName(), d.getTableName());
      totalSize = totalSize * blockCount;
    }
    int count = 0;
    for (HyperTableCube cube:cubes) {
      int volume = 1;
      for (Dimension d:cube.getDimensions()) {
        volume = volume * d.length();
      }
      count += volume;
    }
    return totalSize/count;
  }
}
