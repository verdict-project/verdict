package org.verdictdb.core.execution;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.CreateTableAsSelectQuery;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.sql.QueryToSql;
import org.verdictdb.exception.VerdictDbException;

public class CreateTableAsSelectExecutionNode extends QueryExecutionNodeWithPlaceHolders {
  
  String newTableSchemaName;
  
  String newTableName;
  
  String scratchpadSchemaName;
  
  static int tempTableNameNum = 0;
  
  protected CreateTableAsSelectExecutionNode(String scratchpadSchemaName) {
    super();
    this.scratchpadSchemaName = scratchpadSchemaName;
    
    Pair<String, String> tempTableFullName = generateTempTableName();
    this.newTableSchemaName = tempTableFullName.getLeft();
    this.newTableName = tempTableFullName.getRight();
  }
  
//  public void setNewTableSchemaName(String schemaName) {
//    this.newTableSchemaName = schemaName;
//  }
//  
//  public void setNewTableName(String tableName) {
//    this.newTableName = tableName;
//  }
  
  public static CreateTableAsSelectExecutionNode create(SelectQuery query, String scratchpadSchemaName) {
    CreateTableAsSelectExecutionNode node = new CreateTableAsSelectExecutionNode(scratchpadSchemaName);
    node.setQuery(query);
    return node;
  }
  
  public void setQuery(SelectQuery query) {
    CreateTableAsSelectQuery createQuery = new CreateTableAsSelectQuery(newTableSchemaName, newTableName, query);
    this.query = createQuery;
  }
  
  public SelectQuery getQuery() {
    return (SelectQuery) query;
  }

  @Override
  public ExecutionResult executeNode(DbmsConnection conn, List<ExecutionResult> downstreamResults) {
    super.executeNode(conn, downstreamResults);
    
//    CreateTableAsSelectQuery createQuery = new CreateTableAsSelectQuery(schemaName, tableName, query);
//    CreateTableToSql toSql = new CreateTableToSql(conn.getSyntax());
    try {
      String sql = QueryToSql.convert(conn.getSyntax(), query);
      conn.executeUpdate(sql);
    } catch (VerdictDbException e) {
      e.printStackTrace();
    }
    
    // write the result
    ExecutionResult result = new ExecutionResult();
    result.setKeyValue("schemaName", newTableSchemaName);
    result.setKeyValue("tableName", newTableName);
    return result;
  }
  
  Pair<String, String> generateTempTableName() {
    return Pair.of(scratchpadSchemaName, String.format("verdictdbtemptable_%d", tempTableNameNum++));
  }

}
