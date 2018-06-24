package org.verdictdb.core.execution;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.CreateTableAsSelectQuery;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.sql.CreateTableToSql;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.sql.syntax.SyntaxAbstract;

import java.util.ArrayList;
import java.util.List;

public class SelectAllExecutionNode extends QueryExecutionNode {

  private List<String> tempTableNames = new ArrayList<>();

  private SyntaxAbstract syntax;

  private SelectQuery query;

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

  public void addQuery(SelectQuery query) {
    this.query = query;
    generateDependency();
  }

  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    // TODO: how to write this?
    // write the result
    ExecutionResult result = new ExecutionResult();
    for (String t:tempTableNames){
      String sql = "select * from " + t;
      DbmsQueryResult queryResult = conn.executeQuery(sql);
    }
    return result;
  }

  private void generateDependency() {
    String temptableName = QueryExecutionPlan.generateTempTableName();
    if (query.isAggregateQuery()) {
      addDependency(new AggExecutionNode(conn, scratchpadSchemaName, temptableName, query));
    }
    else {
      addDependency(new ProjectionExecutionNode(conn, scratchpadSchemaName, temptableName, query));
    }
    tempTableNames.add(temptableName);
  }

}