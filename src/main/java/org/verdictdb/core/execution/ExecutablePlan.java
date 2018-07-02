package org.verdictdb.core.execution;

import java.util.List;

public interface ExecutablePlan {
  
  public List<Integer> getNodeGroupIDs();
  
  public List<ExecutableNode> getNodesInGroup(int groupId);

  public ExecutableNode getReportingNode();

}
