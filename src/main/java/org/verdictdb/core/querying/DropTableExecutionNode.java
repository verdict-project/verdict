package org.verdictdb.core.querying;

import java.util.List;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.DropTableQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

public class DropTableExecutionNode extends ExecutableNodeBase {
  
  public DropTableExecutionNode() {
    super();
  }
  
  public static DropTableExecutionNode create() {
    DropTableExecutionNode node = new DropTableExecutionNode();
    return node;
  }
  
  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
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
  public ExecutableNodeBase deepcopy() {
    DropTableExecutionNode node = new DropTableExecutionNode();
    copyFields(this, node);
    return node;
  }
  
  void copyFields(DropTableExecutionNode from, DropTableExecutionNode to) {
    super.copyFields(from, to);
  }

}
