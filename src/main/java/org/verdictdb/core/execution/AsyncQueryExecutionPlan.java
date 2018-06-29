package org.verdictdb.core.execution;

import java.util.List;

import org.verdictdb.core.execution.ola.AggExecutionNodeBlock;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;

public class AsyncQueryExecutionPlan extends QueryExecutionPlan {

  private AsyncQueryExecutionPlan(String scratchpadSchemaName, ScrambleMeta scrambleMeta)
      throws VerdictDBException {
    super(scratchpadSchemaName, scrambleMeta);
  }
  
  public static AsyncQueryExecutionPlan create(QueryExecutionPlan plan) throws VerdictDBException {
    if (plan instanceof AsyncQueryExecutionPlan) {
      System.err.println("It is already an asyncronous plan.");
      throw new VerdictDBTypeException(plan);
    }
    
    AsyncQueryExecutionPlan asyncPlan = new AsyncQueryExecutionPlan(plan.getScratchpadSchemaName(), plan.getScrambleMeta());
    QueryExecutionNode newRoot = makeAsyncronousAggIfAvailable(plan.root);
    asyncPlan.setRootNode(newRoot);
    return asyncPlan;
  }
  
  /**
  *
  * @param root The root execution node of ALL nodes (i.e., not just the top agg node)
  * @return
  * @throws VerdictDBException
  */
 static QueryExecutionNode makeAsyncronousAggIfAvailable(QueryExecutionNode root) throws VerdictDBException {
   List<AggExecutionNodeBlock> aggBlocks = root.identifyTopAggBlocks();

   // converted nodes should be used in place of the original nodes.
   for (int i = 0; i < aggBlocks.size(); i++) {
     AggExecutionNodeBlock nodeBlock = aggBlocks.get(i);
     QueryExecutionNode oldNode = nodeBlock.getBlockRootNode();
     QueryExecutionNode newNode = nodeBlock.convertToProgressiveAgg();

     List<QueryExecutionNode> parents = oldNode.getParents();
     for (QueryExecutionNode parent : parents) {
       List<QueryExecutionNode> parentDependants = parent.getDependents();
       int idx = parentDependants.indexOf(oldNode);
       parentDependants.remove(idx);
       parentDependants.add(idx, newNode);
     }
   }

   return root;
 }

}
