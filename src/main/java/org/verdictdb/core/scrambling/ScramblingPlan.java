/*
 * Copyright 2018 University of Michigan
 *
 * You must contact Barzan Mozafari (mozafari@umich.edu) or Yongjoo Park (pyongjoo@umich.edu) to discuss
 * how you could use, modify, or distribute this code. By default, this code is not open-sourced and we do
 * not license this code.
 */

package org.verdictdb.core.scrambling;

import java.util.List;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutableNode;
import org.verdictdb.core.execution.ExecutablePlan;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.sqlobject.SqlConvertible;

/**
 * Execution plan for scrambling.
 * 
 * This plan consists of three nodes:
 * <ol>
 * <li>metadata retrieval node</li>
 * <li>statistics computation node</li>
 * <li>actual scramble table creation node</li>
 * </ol>
 * 
 * Those nodes should be provided by the ScramblingMethod instance
 * 
 * @author Yongjoo Park
 *
 */
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
    int tileCount = method.getTierCount(statistics);
    List<String> tileExpressions = method.getTierExpressions(statistics);
    
    
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
