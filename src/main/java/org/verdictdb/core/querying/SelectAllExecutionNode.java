package org.verdictdb.core.querying;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

/**
 * 
 * @author Yongjoo Park
 * TODO: we need to also include order-by and limit
 */
public class SelectAllExecutionNode extends QueryNodeWithPlaceHolders {
  
  private SelectAllExecutionNode(SelectQuery query){
    super(query);
  }

  public static SelectAllExecutionNode create(IdCreator namer, SelectQuery query) throws VerdictDBValueException {
    SelectAllExecutionNode selectAll = new SelectAllExecutionNode(null);
    Pair<BaseTable, SubscriptionTicket> baseAndSubscriptionTicket = selectAll.createPlaceHolderTable("t");
    SelectQuery selectQuery = SelectQuery.create(new AsteriskColumn(), baseAndSubscriptionTicket.getLeft());
    selectAll.setSelectQuery(selectQuery);
    
//    Pair<String, String> tempTableFullName = plan.generateTempTableName();
//    String schemaName = tempTableFullName.getLeft();
//    String tableName = tempTableFullName.getRight();
    
    if (query.isSupportedAggregate()) {
      AggExecutionNode dependent = AggExecutionNode.create(namer, query);
      dependent.registerSubscriber(baseAndSubscriptionTicket.getRight());
//      selectAll.addDependency(dependent);
    }
    else {
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
//  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) 
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
    SelectAllExecutionNode node = new SelectAllExecutionNode(selectQuery);
    copyFields(this, node);
    return node;
  }

  void copyFields(CreateTableAsSelectNode from, CreateTableAsSelectNode to) {
    super.copyFields(from, to);
  }

}