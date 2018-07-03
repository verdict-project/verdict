package org.verdictdb.core.querying.ola;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.querying.BaseQueryNode;
import org.verdictdb.core.querying.CreateTableAsSelectNode;
import org.verdictdb.core.querying.TempIdCreator;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertable;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

public class AggCombinerExecutionNode extends CreateTableAsSelectNode {

  List<HyperTableCube> cubes = new ArrayList<>();

  private AggCombinerExecutionNode(TempIdCreator namer) {
    super(namer, null);
  }
  
  public static AggCombinerExecutionNode create(
      TempIdCreator namer,
      BaseQueryNode leftQueryExecutionNode,
      BaseQueryNode rightQueryExecutionNode) throws VerdictDBValueException {
    AggCombinerExecutionNode node = new AggCombinerExecutionNode(namer);
    
//    SelectQuery leftQuery = queryExecutionNode.getSelectQuery();
    SelectQuery rightQuery = rightQueryExecutionNode.getSelectQuery();   // the right one is the aggregate query
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
  public SqlConvertable createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    for (ExecutionInfoToken token:tokens) {
      if (token.getValue("hyperTableCube")!=null) {
        cubes.addAll((List<HyperTableCube>) token.getValue("hyperTableCube"));
      }
    }
    return super.createQuery(tokens);
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = super.createToken(result);
    if (!cubes.isEmpty()) {
      token.setKeyValue("hyperTableCube", cubes);
    }
    token.setKeyValue("dependent", this);
    return token;
  }

}
