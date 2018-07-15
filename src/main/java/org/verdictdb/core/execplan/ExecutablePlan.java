package org.verdictdb.core.execplan;

import java.util.List;
import java.util.Set;

public interface ExecutablePlan {
  
  public Set<Integer> getNodeGroupIDs();
  
  public List<ExecutableNode> getNodesInGroup(int groupId);

  public ExecutableNode getReportingNode();

}
