package org.verdictdb.core.execution;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.query.AsteriskColumn;
import org.verdictdb.core.query.BaseTable;
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

  public static SelectAllExecutionNode create(QueryExecutionPlan plan, SelectQuery query) {
    SelectAllExecutionNode selectAll = new SelectAllExecutionNode();
    Pair<BaseTable, ExecutionTokenQueue> baseAndQueue = selectAll.createPlaceHolderTable("t");
    SelectQuery selectQuery = SelectQuery.create(new AsteriskColumn(), baseAndQueue.getLeft());
    selectAll.setSelectQuery(selectQuery);
    
//    Pair<String, String> tempTableFullName = plan.generateTempTableName();
//    String schemaName = tempTableFullName.getLeft();
//    String tableName = tempTableFullName.getRight();
    
    if (query.isAggregateQuery()) {
      AggExecutionNode dependent = AggExecutionNode.create(plan, query);
      dependent.addBroadcastingQueue(baseAndQueue.getRight());
      selectAll.addDependency(dependent);
    }
    else {
      ProjectionExecutionNode dependent = ProjectionExecutionNode.create(plan, query);
      dependent.addBroadcastingQueue(baseAndQueue.getRight());
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