package org.verdictdb.core.execution;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.query.AsteriskColumn;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.sql.QueryToSql;
import org.verdictdb.core.sql.SelectQueryToSql;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.sql.syntax.SyntaxAbstract;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

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
    selectAll.setQuery(selectQuery);
    ExecutionResultQueue queue = selectAll.generateListeningQueue();
    
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
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    super.executeNode(downstreamResults);
    try {
      String sql = QueryToSql.convert(conn.getSyntax(), query);
      DbmsQueryResult queryResult = conn.executeQuery(sql);
      ExecutionResult result = new ExecutionResult();
      result.setKeyValue("queryResult", queryResult);
      return result;
      
    } catch (VerdictDbException e) {
      e.printStackTrace();
    }
    return ExecutionResult.empty();
  }

//  public List<DbmsQueryResult> getDbmsQueryResults() {
//    return dbmsQueryResults;
//  }

}