package org.verdictdb.core.querying.ola;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.querying.CreateTableAsSelectNode;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.querying.SubscriptionTicket;
import org.verdictdb.core.querying.TempIdCreator;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

public class AggCombinerExecutionNode extends CreateTableAsSelectNode {

  private AggCombinerExecutionNode(TempIdCreator namer) {
    super(namer, null);
  }
  
  public static AggCombinerExecutionNode create(
      TempIdCreator namer,
      ExecutableNodeBase leftQueryExecutionNode,
      ExecutableNodeBase rightQueryExecutionNode) throws VerdictDBValueException {
    AggCombinerExecutionNode node = new AggCombinerExecutionNode(namer);
    
//    SelectQuery leftQuery = queryExecutionNode.getSelectQuery();
    SelectQuery rightQuery = ((QueryNodeBase) rightQueryExecutionNode).getSelectQuery();   // the right one is the aggregate query
    String leftAliasName = namer.generateAliasName();
    String rightAliasName = namer.generateAliasName();
    
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
    
    Pair<BaseTable, SubscriptionTicket> leftBaseAndTicket = node.createPlaceHolderTable(leftAliasName);
    Pair<BaseTable, SubscriptionTicket> rightBaseAndTicket = node.createPlaceHolderTable(rightAliasName);
    SelectQuery joinQuery = SelectQuery.create(
        allItems, 
        Arrays.<AbstractRelation>asList(leftBaseAndTicket.getLeft(), rightBaseAndTicket.getLeft()));
    for (String a : groupAliasNames) {
      joinQuery.addFilterByAnd(
          ColumnOp.equal(new BaseColumn(leftAliasName, a), new BaseColumn(rightAliasName, a)));
    }
    leftQueryExecutionNode.registerSubscriber(leftBaseAndTicket.getRight());
    rightQueryExecutionNode.registerSubscriber(rightBaseAndTicket.getRight());
    
    node.setSelectQuery(joinQuery);
//    node.addDependency(leftQueryExecutionNode);
//    node.addDependency(rightQueryExecutionNode);
    return node;
  }
  
  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    return super.createQuery(tokens);
  }
  
  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    return super.createToken(result);
  }

//  @Override
//  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) 
//      throws VerdictDBException {
//    ExecutionInfoToken token = super.executeNode(conn, downstreamResults);
//    /*
//    List<HyperTableCube> cubes = new ArrayList<>();
//    for (ExecutionInfoToken downstreamResult:downstreamResults) {
//      cubes.addAll((List<HyperTableCube>) downstreamResult.getValue("hyperTableCube"));
//    }
//    token.setKeyValue("hyperTableCube", cubes);
//    */
//    return token;
//  }

}
