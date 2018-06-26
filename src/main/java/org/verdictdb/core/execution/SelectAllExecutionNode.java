package org.verdictdb.core.execution;

import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.query.AsteriskColumn;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.sql.QueryToSql;
import org.verdictdb.exception.VerdictDBException;

/**
 * 
 * @author Yongjoo Park
 * TODO: we need to also include order-by and limit
 */
public class SelectAllExecutionNode extends QueryExecutionNodeWithPlaceHolders {

//  private List<String> tempTableNames = new ArrayList<>();
//
//  private List<DbmsQueryResult> dbmsQueryResults = new ArrayList<>();

//  BlockingDeque<ExecutionResult> queue = new LinkedBlockingDeque<>();
  
  private SelectAllExecutionNode(){
    super(null);
  }

  public static SelectAllExecutionNode create(SelectQuery query, String scratchpadSchemaName) {
    SelectAllExecutionNode selectAll = new SelectAllExecutionNode();
    SelectQuery selectQuery = SelectQuery.create(new AsteriskColumn(), selectAll.createPlaceHolderTable("t"));
    selectAll.setSelectQuery(selectQuery);
    ExecutionTokenQueue queue = selectAll.generateListeningQueue();
    
//    Pair<String, String> tempTableFullName = plan.generateTempTableName();
//    String schemaName = tempTableFullName.getLeft();
//    String tableName = tempTableFullName.getRight();
    
    if (query.isAggregateQuery()) {
      AggExecutionNode dependent = AggExecutionNode.create(query, scratchpadSchemaName);
      dependent.addBroadcastingQueue(queue);
      selectAll.addDependency(dependent);
    }
    else {
      ProjectionExecutionNode dependent = ProjectionExecutionNode.create(query, scratchpadSchemaName);
      dependent.addBroadcastingQueue(queue);
      selectAll.addDependency(dependent);
    }
    
    return selectAll;
  }

  @Override
  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) 
      throws VerdictDBException {
    super.executeNode(conn, downstreamResults);
    try {
      String sql = QueryToSql.convert(conn.getSyntax(), selectQuery);
      DbmsQueryResult queryResult = conn.executeQuery(sql);
      ExecutionInfoToken result = new ExecutionInfoToken();
      result.setKeyValue("queryResult", queryResult);
      return result;
      
    } catch (VerdictDBException e) {
      e.printStackTrace();
    }
    return ExecutionInfoToken.empty();
  }

  @Override
  public QueryExecutionNode deepcopy() {
    SelectAllExecutionNode node = new SelectAllExecutionNode();
    copyFields(this, node);
    return node;
  }

  void copyFields(CreateTableAsSelectExecutionNode from, CreateTableAsSelectExecutionNode to) {
    super.copyFields(from, to);
  }

}