package org.verdictdb.core.querying;

import java.util.List;

import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.DropTableQuery;
import org.verdictdb.core.sqlobject.SqlConvertable;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;
import org.verdictdb.sqlreader.QueryToSql;

public class DropTableExecutionNode extends BaseQueryNode {
  
  public DropTableExecutionNode() {
    super();
  }
  
  public static DropTableExecutionNode create() {
    DropTableExecutionNode node = new DropTableExecutionNode();
    return node;
  }
  
  @Override
  public SqlConvertable createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    try {
      if (tokens.size() == 0) {
        throw new VerdictDBValueException("No table to drop!");
      }
    } catch (VerdictDBException e) {
      e.printStackTrace();
    }
    
    ExecutionInfoToken result = tokens.get(0);
    String schemaName = (String) result.getValue("schemaName");
    String tableName = (String) result.getValue("tableName");
    DropTableQuery dropQuery = new DropTableQuery(schemaName, tableName);
    return dropQuery;
  }
  
  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    return ExecutionInfoToken.empty();
  }

  @Override
  public BaseQueryNode deepcopy() {
    DropTableExecutionNode node = new DropTableExecutionNode();
    copyFields(this, node);
    return node;
  }
  
  void copyFields(DropTableExecutionNode from, DropTableExecutionNode to) {
    super.copyFields(from, to);
  }

}
