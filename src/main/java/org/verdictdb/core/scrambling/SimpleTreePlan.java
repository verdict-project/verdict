package org.verdictdb.core.scrambling;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.verdictdb.core.execplan.ExecutableNode;
import org.verdictdb.core.execplan.ExecutablePlan;
import org.verdictdb.core.querying.ExecutableNodeBase;

/**
 * Easily creates a plan given a root node.
 * 
 * @author Yongjoo Park
 *
 */
public class SimpleTreePlan implements ExecutablePlan {
  
  ExecutableNodeBase root;
  
  public SimpleTreePlan(ExecutableNodeBase root) {
    this.root = root;
  }

  @Override
  public List<Integer> getNodeGroupIDs() {
    return Arrays.asList(0);
  }

  @Override
  public List<ExecutableNode> getNodesInGroup(int groupId) {
    List<ExecutableNode> nodes = new ArrayList<>();
    List<ExecutableNodeBase> pool = new LinkedList<>();
    pool.add(root);
    while (!pool.isEmpty()) {
      ExecutableNodeBase n = pool.remove(0);
      if (nodes.contains(n)) {
        continue;
      }
      nodes.add(n);
      pool.addAll(n.getExecutableNodeBaseDependents());
    }
    return nodes;
  }

  @Override
  public ExecutableNode getReportingNode() {
    return root;
  }

}
