package org.verdictdb.core.execution;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.sql.syntax.SyntaxAbstract;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class SelectAllExecutionNode extends QueryExecutionNode {

  private List<String> tempTableNames = new ArrayList<>();

  private SyntaxAbstract syntax;

  private SelectQuery query;

  private List<DbmsQueryResult> dbmsQueryResults = new ArrayList<>();

  BlockingDeque<ExecutionResult> queue = new LinkedBlockingDeque<>();

  private String scratchpadSchemaName;

  public SelectAllExecutionNode(DbmsConnection conn, SyntaxAbstract syntax, SelectQuery query, String scratchpadSchemaName){
    super(conn, query);
    this.syntax = syntax;
    this.query = query;
    this.scratchpadSchemaName = scratchpadSchemaName;
    generateDependency();
  };

  public void addtempTableName(String schema, String tempTableName) {
    tempTableNames.add( syntax.getQuoteString() + schema + syntax.getQuoteString() + "." + syntax.getQuoteString() +
        tempTableName + syntax.getQuoteString());
  }

  public List<String> getTempTableNames() {
    return tempTableNames;
  }

  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    for (ExecutionResult result:queue) {
      addtempTableName((String)result.getValue("schemaName"), (String) result.getValue("tableName"));
    }
    for (String t:tempTableNames){
      String sql = "select * from " + t;
      DbmsQueryResult queryResult = conn.executeQuery(sql);
      dbmsQueryResults.add(queryResult);
    }

    return new ExecutionResult();
  }

  public List<DbmsQueryResult> getDbmsQueryResults() {
    return dbmsQueryResults;
  }

  private void generateDependency() {
    String temptableName = QueryExecutionPlan.generateTempTableName();
    if (query.isAggregateQuery()) {
      AggExecutionNode dependent = new AggExecutionNode(conn, scratchpadSchemaName, temptableName, query);
      dependent.addBroadcastingQueue(queue);
      addDependency(dependent);
    }
    else {
      ProjectionExecutionNode dependent = new ProjectionExecutionNode(conn, scratchpadSchemaName, temptableName, query);
      dependent.addBroadcastingQueue(queue);
      addDependency(dependent);
    }
  }

}