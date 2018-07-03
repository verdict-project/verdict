package org.verdictdb.core.scrambling;

import java.util.Arrays;
import java.util.List;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutableNode;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.sqlobject.SqlConvertable;
import org.verdictdb.exception.VerdictDBException;

public class ScramblingNode implements ExecutableNode {
  
  public ScramblingNode(ScramblingMethod method) {
    
  }

  @Override
  public List<ExecutionTokenQueue> getSourceQueues() {
    return Arrays.<ExecutionTokenQueue>asList();
  }

  @Override
  public List<ExecutionTokenQueue> getDestinationQueues() {
    return Arrays.<ExecutionTokenQueue>asList();
  }

  @Override
  public SqlConvertable createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    return ExecutionInfoToken.empty();
  }

  @Override
  public int getDependentNodeCount() {
    return 0;
  }

}
