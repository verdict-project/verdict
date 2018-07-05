package org.verdictdb.core.querying.ola;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
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

  AggMeta aggMeta = new AggMeta();

  private AggCombinerExecutionNode(TempIdCreator namer) {
    super(namer, null);
  }
  
  public static AggCombinerExecutionNode create(
      TempIdCreator namer,
      ExecutableNodeBase leftQueryExecutionNode,
      ExecutableNodeBase rightQueryExecutionNode) throws VerdictDBValueException {
    AggCombinerExecutionNode node = new AggCombinerExecutionNode(namer);
    
    SelectQuery rightQuery = ((QueryNodeBase) rightQueryExecutionNode).getSelectQuery();   // the right one is the aggregate query
    String leftAliasName = namer.generateAliasName();
    String rightAliasName = namer.generateAliasName();
    
    // create placeholders to use
    Pair<BaseTable, SubscriptionTicket> leftBaseAndTicket = node.createPlaceHolderTable(leftAliasName);
    Pair<BaseTable, SubscriptionTicket> rightBaseAndTicket = node.createPlaceHolderTable(rightAliasName);
    
    // compose a join query
    SelectQuery joinQuery = composeJoinQuery(rightQuery, leftBaseAndTicket.getLeft(), rightBaseAndTicket.getLeft());
    
    leftQueryExecutionNode.registerSubscriber(leftBaseAndTicket.getRight());
    rightQueryExecutionNode.registerSubscriber(rightBaseAndTicket.getRight());
    
    node.setSelectQuery(joinQuery);
    return node;
  }
  
  /**
   * Composes a query that joins two tables. The select list is inferred from a given query.
   * 
   * @param rightQuery The query from which to infer a select list
   * @param leftBase
   * @param rightBase
   * @return
   */
  static SelectQuery composeJoinQuery(
      SelectQuery rightQuery,
      BaseTable leftBase,
      BaseTable rightBase) {
    
    // retrieves the select list
    List<String> groupAliasNames = new ArrayList<>();
    List<String> measureAliasNames = new ArrayList<>();
    for (SelectItem item : rightQuery.getSelectList()) {
      if (item.isAggregateColumn()) {
        measureAliasNames.add(((AliasedColumn) item).getAliasName());
      } else {
        groupAliasNames.add(((AliasedColumn) item).getAliasName());
      }
    }
    
    // replace the alias names of those select items
    String leftAliasName = leftBase.getAliasName().get();
    String rightAliasName = rightBase.getAliasName().get();
    
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
    
    // finally, creates a join query
    SelectQuery joinQuery = SelectQuery.create(
        allItems, 
        Arrays.<AbstractRelation>asList(leftBase, rightBase));
    for (String a : groupAliasNames) {
      joinQuery.addFilterByAnd(
          ColumnOp.equal(new BaseColumn(leftAliasName, a), new BaseColumn(rightAliasName, a)));
    }
    
    return joinQuery;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    for (ExecutionInfoToken token:tokens) {
      AggMeta aggMeta = (AggMeta) token.getValue("aggMeta");
      this.aggMeta.getCubes().addAll(aggMeta.getCubes());
      this.aggMeta.setAggAlias(aggMeta.getAggAlias());
    }
    return super.createQuery(tokens);
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = super.createToken(result);
    token.setKeyValue("aggMeta", aggMeta);
    token.setKeyValue("dependent", this);
    return token;
  }

}
