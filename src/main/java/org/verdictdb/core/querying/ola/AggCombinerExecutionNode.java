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

package org.verdictdb.core.querying.ola;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.*;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;

import java.util.ArrayList;
import java.util.List;

public class AggCombinerExecutionNode extends CreateTableAsSelectNode {

  private static final long serialVersionUID = -5083977853340736042L;
  
  private static final String unionTableAlias = "unionTable";

  private AggCombinerExecutionNode(IdCreator namer) {
    super(namer, null);
  }

  public static AggCombinerExecutionNode create(
      IdCreator namer,
      ExecutableNodeBase leftQueryExecutionNode,
      ExecutableNodeBase rightQueryExecutionNode) {

    AggCombinerExecutionNode node = new AggCombinerExecutionNode(namer);

    SelectQuery rightQuery =
        ((QueryNodeBase) rightQueryExecutionNode)
            .getSelectQuery(); // the right one is the aggregate query
    String leftAliasName = namer.generateAliasName();
    String rightAliasName = namer.generateAliasName();

    // create placeholders to use
    Pair<BaseTable, SubscriptionTicket> leftBaseAndTicket =
        node.createPlaceHolderTable(leftAliasName);
    Pair<BaseTable, SubscriptionTicket> rightBaseAndTicket =
        node.createPlaceHolderTable(rightAliasName);

    // compose a union query to sum the results from two children aggregate nodes
    SelectQuery unionQuery =
        composeUnionQuery(rightQuery, leftBaseAndTicket.getLeft(), rightBaseAndTicket.getLeft());

    leftQueryExecutionNode.registerSubscriber(leftBaseAndTicket.getRight());
    rightQueryExecutionNode.registerSubscriber(rightBaseAndTicket.getRight());

    node.setSelectQuery(unionQuery);
    
    // Update aggMeta of this node
    updateAggMeta(node, leftQueryExecutionNode);
    updateAggMeta(node, rightQueryExecutionNode);
    
    return node;
  }
  
  private static void updateAggMeta(AggCombinerExecutionNode node, ExecutableNodeBase child) {
    AggMeta nodeMeta = node.getAggMeta();
    AggMeta childAggMeta = child.getAggMeta();
  
    nodeMeta.getCubes().addAll(childAggMeta.getCubes());
    nodeMeta.setAggAlias(childAggMeta.getAggAlias());
    nodeMeta.setOriginalSelectList(childAggMeta.getOriginalSelectList());
    nodeMeta.setAggColumn(childAggMeta.getAggColumn());
    nodeMeta.setAggColumnAggAliasPair(childAggMeta.getAggColumnAggAliasPair());
    nodeMeta.setAggColumnAggAliasPairOfMaxMin(childAggMeta.getAggColumnAggAliasPairOfMaxMin());
    nodeMeta.setMaxminAggAlias(childAggMeta.getMaxminAggAlias());
    nodeMeta.setTierColumnForScramble(childAggMeta.getTierColumnForScramble());
    
    node.setAggMeta(nodeMeta);
  }

  /**
   * Composes a query that unions two aggregate results. The select list is inferred from a given query.
   *
   * For min and max aggregates, the extreme of two downstream aggregates is taken.
   *
   * For sum and count aggregates, their sum is computed.
   *
   * @param rightQuery The query from which to infer a select list
   * @param leftBase The left individual aggregate
   * @param rightBase The right individual aggregate
   * @return The query that properly unions two aggregate queries.
   */
  static SelectQuery composeUnionQuery(
      SelectQuery rightQuery, BaseTable leftBase, BaseTable rightBase) {

    List<SelectItem> allItems = new ArrayList<>();
    
    // replace the select list
    List<GroupingAttribute> groupingAttributes = new ArrayList<>();
    for (SelectItem item : rightQuery.getSelectList()) {
      // an aggregate column is either max/min-ed or summed.
      if (item.isAggregateColumn()) {
        if (!(item instanceof AliasedColumn)
                || !(((AliasedColumn) item).getColumn() instanceof ColumnOp)) {
          continue;
        }
        
        ColumnOp column = (ColumnOp) ((AliasedColumn) item).getColumn();
        // for min or max, we take min or max of the two aggregates
        if (column.getOpType().equals("max") || column.getOpType().equals("min")) {
          AliasedColumn newColumn = new AliasedColumn(
              new ColumnOp(
                  column.getOpType(),
                  new BaseColumn(unionTableAlias, ((AliasedColumn) item).getAliasName())),
              ((AliasedColumn) item).getAliasName());
          allItems.add(newColumn);
        }
        // for other aggregates (i.e., sum and count), we sum them.
        else {
          AliasedColumn newColumn = new AliasedColumn(
              ColumnOp.sum(new BaseColumn(unionTableAlias, ((AliasedColumn) item).getAliasName())),
              ((AliasedColumn) item).getAliasName());
          allItems.add(newColumn);
        }
      }
      else {
        UnnamedColumn col =  new BaseColumn(unionTableAlias, ((AliasedColumn) item).getAliasName());
        allItems.add(
            new AliasedColumn(
                col,
                ((AliasedColumn) item).getAliasName()));
        groupingAttributes.add(col);
      }
    }

    SelectQuery left = SelectQuery.create(new AsteriskColumn(), leftBase);
    SelectQuery right = SelectQuery.create(new AsteriskColumn(), rightBase);
//    SetOperationRelation newBase =
//        new SetOperationRelation(left, right, SetOperationRelation.SetOpType.unionAll);
    
    // we place the right one on top so that the generated table uses its column names
    SetOperationRelation newBase =
        new SetOperationRelation(right, left, SetOperationRelation.SetOpType.unionAll);
    newBase.setAliasName(unionTableAlias);
    SelectQuery unionQuery = SelectQuery.create(allItems, newBase);
    for (GroupingAttribute a : groupingAttributes) {
      unionQuery.addGroupby(a);
    }
    /*
    // finally, creates a join query
    SelectQuery joinQuery = SelectQuery.create(
        allItems,
        Arrays.<AbstractRelation>asList(leftBase, rightBase));
    for (String a : groupAliasNames) {
      joinQuery.addFilterByAnd(
          ColumnOp.equal(new BaseColumn(leftAliasName, a), new BaseColumn(rightAliasName, a)));
    }
    */

    return unionQuery;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    return super.createQuery(tokens);
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = super.createToken(result);
    token.setKeyValue("aggMeta", aggMeta);
    token.setKeyValue("dependentQuery", selectQuery);
    return token;
  }
}
