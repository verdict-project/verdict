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

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

import java.util.List;

/** @author Yongjoo Park */
public class SelectAllExecutionNode extends QueryNodeWithPlaceHolders {

  public SelectAllExecutionNode(IdCreator idCreator, SelectQuery query) {
    super(idCreator, query);
  }
  
  public SelectAllExecutionNode(int uniqueId, SelectQuery query) {
    super(uniqueId, query);
  }

  public static SelectAllExecutionNode create(IdCreator namer, SelectQuery query)
      throws VerdictDBValueException {
    SelectAllExecutionNode selectAll = new SelectAllExecutionNode(namer, null);
    Pair<BaseTable, SubscriptionTicket> baseAndSubscriptionTicket =
        selectAll.createPlaceHolderTable("t");
    SelectQuery selectQuery =
        SelectQuery.create(new AsteriskColumn(), baseAndSubscriptionTicket.getLeft());
    selectQuery.addOrderby(query.getOrderby());
    if (query.getLimit().isPresent()) selectQuery.addLimit(query.getLimit().get());
    selectAll.setSelectQuery(selectQuery);

    //    Pair<String, String> tempTableFullName = plan.generateTempTableName();
    //    String schemaName = tempTableFullName.getLeft();
    //    String tableName = tempTableFullName.getRight();

    if (query.isSupportedAggregate()) {
      AggExecutionNode dependent = AggExecutionNode.create(namer, query);
      dependent.registerSubscriber(baseAndSubscriptionTicket.getRight());
      //      selectAll.addDependency(dependent);
    } else {
      ProjectionNode dependent = ProjectionNode.create(namer, query);
      dependent.registerSubscriber(baseAndSubscriptionTicket.getRight());
      //      selectAll.addDependency(dependent);
    }

    return selectAll;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    return super.createQuery(tokens);
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("queryResult", result);
    return token;
  }

  //  @Override
  //  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken>
  // downstreamResults)
  //      throws VerdictDBException {
  //    super.executeNode(conn, downstreamResults);
  //    try {
  //      String sql = QueryToSql.convert(conn.getSyntax(), selectQuery);
  //      DbmsQueryResult queryResult = conn.executeQuery(sql);
  //      ExecutionInfoToken result = new ExecutionInfoToken();
  //      result.setKeyValue("queryResult", queryResult);
  //      return result;
  //
  //    } catch (VerdictDBException e) {
  //      e.printStackTrace();
  //    }
  //    return ExecutionInfoToken.empty();
  //  }

  @Override
  public ExecutableNodeBase deepcopy() {
    SelectAllExecutionNode node = new SelectAllExecutionNode(uniqueId, selectQuery);
    copyFields(this, node);
    return node;
  }

  void copyFields(CreateTableAsSelectNode from, CreateTableAsSelectNode to) {
    super.copyFields(from, to);
  }
}
