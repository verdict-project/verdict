package org.verdictdb.core.querynode.ola;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.querynode.CreateTableAsSelectExecutionNode;
import org.verdictdb.core.querynode.QueryExecutionNode;
import org.verdictdb.core.querynode.QueryExecutionPlan;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

public class AggCombinerExecutionNode extends CreateTableAsSelectExecutionNode {

  private AggCombinerExecutionNode(QueryExecutionPlan plan) {
    super(plan);
  }
  
  public static AggCombinerExecutionNode create(
      QueryExecutionPlan plan,
      QueryExecutionNode leftQueryExecutionNode,
      QueryExecutionNode rightQueryExecutionNode) throws VerdictDBValueException {
    AggCombinerExecutionNode node = new AggCombinerExecutionNode(plan);
    
//    SelectQuery leftQuery = queryExecutionNode.getSelectQuery();
    SelectQuery rightQuery = rightQueryExecutionNode.getSelectQuery();   // the right one is the aggregate query
    String leftAliasName = plan.generateAliasName();
    String rightAliasName = plan.generateAliasName();
    
    List<String> groupAliasNames = new ArrayList<>();
    List<String> measureAliasNames = new ArrayList<>();
    for (SelectItem item : rightQuery.getSelectList()) {
      if (item.isAggregateColumn()) {
        measureAliasNames.add(((AliasedColumn) item).getAliasName());
      } else {
        groupAliasNames.add(((AliasedColumn) item).getAliasName());
      }
    }
    
    // compose a join query
    List<SelectItem> groupItems = new ArrayList<>();
    List<SelectItem> measureItems = new ArrayList<>();
    for (String a : groupAliasNames) {
      groupItems.add(new AliasedColumn(new BaseColumn(leftAliasName, a), a));
    }
    for (String a : measureAliasNames) {
      measureItems.add(new AliasedColumn(
          ColumnOp.add(new BaseColumn(leftAliasName, a), new BaseColumn(rightAliasName, a)),
          a));
    }
    List<SelectItem> allItems = new ArrayList<>();
    allItems.addAll(groupItems);
    allItems.addAll(measureItems);
    
    Pair<BaseTable, ExecutionTokenQueue> leftBaseAndQueue = node.createPlaceHolderTable(leftAliasName);
    Pair<BaseTable, ExecutionTokenQueue> rightBaseAndQueue = node.createPlaceHolderTable(rightAliasName);
    SelectQuery joinQuery = SelectQuery.create(
        allItems, 
        Arrays.<AbstractRelation>asList(leftBaseAndQueue.getLeft(), rightBaseAndQueue.getLeft()));
    for (String a : groupAliasNames) {
      joinQuery.addFilterByAnd(
          ColumnOp.equal(new BaseColumn(leftAliasName, a), new BaseColumn(rightAliasName, a)));
    }
    leftQueryExecutionNode.addBroadcastingQueue(leftBaseAndQueue.getRight());
    rightQueryExecutionNode.addBroadcastingQueue(rightBaseAndQueue.getRight());
    
    node.setSelectQuery(joinQuery);
    node.addDependency(leftQueryExecutionNode);
    node.addDependency(rightQueryExecutionNode);
    return node;
  }

  @Override
  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) 
      throws VerdictDBException {
    ExecutionInfoToken token = super.executeNode(conn, downstreamResults);
    /*
    List<HyperTableCube> cubes = new ArrayList<>();
    for (ExecutionInfoToken downstreamResult:downstreamResults) {
      cubes.addAll((List<HyperTableCube>) downstreamResult.getValue("hyperTableCube"));
    }
    token.setKeyValue("hyperTableCube", cubes);
    */
    return token;
  }

}
