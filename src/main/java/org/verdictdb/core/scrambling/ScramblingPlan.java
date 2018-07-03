package org.verdictdb.core.scrambling;

import java.util.Arrays;
import java.util.List;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutableNode;
import org.verdictdb.core.execution.ExecutablePlan;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

public class ScramblingPlan implements ExecutablePlan {
  
  ScramblingMethod method;
  
  DbmsQueryResult statistics;
  
  private ScramblingPlan(ExecutableNodeBase scrambler, ExecutableNodeBase statRetreival) {
    
    this.method = method;
  }
  
  public static ScramblingPlan create(ScramblingMethod method) {
    return null;
  }
  
  SqlConvertible composeQuery() {
    int tileCount = method.getTileCount(statistics);
    List<String> tileExpressions = method.getTileExpressions(statistics);
    
    
    return null;
  }

  @Override
  public List<Integer> getNodeGroupIDs() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<ExecutableNode> getNodesInGroup(int groupId) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ExecutableNode getReportingNode() {
    // TODO Auto-generated method stub
    return null;
  }

}
