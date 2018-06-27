package org.verdictdb.core.execution;

import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.DropTableQuery;
import org.verdictdb.core.sql.QueryToSql;
import org.verdictdb.exception.VerdictDBValueException;
import org.verdictdb.exception.VerdictDBException;

public class DropTableExecutionNode extends QueryExecutionNode {
  
  public DropTableExecutionNode(QueryExecutionPlan plan) {
    super(plan);
  }
  
  public static DropTableExecutionNode create(QueryExecutionPlan plan) {
    DropTableExecutionNode node = new DropTableExecutionNode(plan);
    return node;
  }

  @Override
  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) {
    try {
      if (downstreamResults.size() == 0) {
        throw new VerdictDBValueException("No table to drop!");
      }
    } catch (VerdictDBException e) {
      e.printStackTrace();
    }
    
    ExecutionInfoToken result = downstreamResults.get(0);
    String schemaName = (String) result.getValue("schemaName");
    String tableName = (String) result.getValue("tableName");
    
    DropTableQuery dropQuery = new DropTableQuery(schemaName, tableName);
    try {
      String sql = QueryToSql.convert(conn.getSyntax(), dropQuery);
      conn.executeUpdate(sql);
    } catch (VerdictDBException e) {
      e.printStackTrace();
    }
    return ExecutionInfoToken.empty();
  }

  @Override
  public QueryExecutionNode deepcopy() {
    DropTableExecutionNode node = new DropTableExecutionNode(plan);
    copyFields(this, node);
    return node;
  }
  
  void copyFields(DropTableExecutionNode from, DropTableExecutionNode to) {
    super.copyFields(from, to);
  }

}
