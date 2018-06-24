package org.verdictdb.core.execution;

import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.CreateTableAsSelectQuery;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.sql.CreateTableToSql;
import org.verdictdb.exception.VerdictDbException;

public class CreateTableAsSelectExecutionNode extends QueryExecutionNode {
  
  String schemaName;
  
  String tableName;
  
  public CreateTableAsSelectExecutionNode(
      DbmsConnection conn,
      String schemaName, 
      String tableName, 
      SelectQuery query) {
    super(conn, query);
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    CreateTableAsSelectQuery createQuery = new CreateTableAsSelectQuery(schemaName, tableName, query);
    CreateTableToSql toSql = new CreateTableToSql(conn.getSyntax());
    try {
      String sql = toSql.toSql(createQuery);
      conn.executeUpdate(sql);
    } catch (VerdictDbException e) {
      e.printStackTrace();
    }
    
    // write the result
    ExecutionResult result = new ExecutionResult();
    result.setKeyValue("schemaName", schemaName);
    result.setKeyValue("tableName", tableName);
    return result;
  }

}
