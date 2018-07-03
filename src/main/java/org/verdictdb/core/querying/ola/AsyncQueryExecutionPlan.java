package org.verdictdb.core.querying.ola;

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.core.querying.AggExecutionNode;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectQuery;
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

    AsyncQueryExecutionPlan asyncPlan = 
        new AsyncQueryExecutionPlan(plan.getScratchpadSchemaName(), plan.getScrambleMeta());
    ExecutableNodeBase newRoot = makeAsyncronousAggIfAvailable(plan.getScrambleMeta(), plan.getRootNode());
    asyncPlan.setRootNode(newRoot);
    return asyncPlan;
  }

  /**
   *
   * @param root The root execution node of ALL nodes (i.e., not just the top agg node)
   * @return
   * @throws VerdictDBException
   */
  static ExecutableNodeBase makeAsyncronousAggIfAvailable(ScrambleMeta scrambleMeta, ExecutableNodeBase root) 
      throws VerdictDBException {
    List<AggExecutionNodeBlock> aggBlocks = identifyTopAggBlocks(scrambleMeta, root);

    // converted nodes should be used in place of the original nodes.
    for (int i = 0; i < aggBlocks.size(); i++) {
      AggExecutionNodeBlock nodeBlock = aggBlocks.get(i);
      ExecutableNodeBase oldNode = nodeBlock.getBlockRootNode();
      ExecutableNodeBase newNode = nodeBlock.convertToProgressiveAgg(scrambleMeta);

      List<ExecutableNodeBase> parents = oldNode.getExecutableNodeBaseParents();
      for (ExecutableNodeBase parent : parents) {
        Integer channel = parent.getChannelForSource(oldNode);
        if (channel == null) {
          // do nothing
        } else {
          parent.cancelSubscriptionTo(oldNode);
          parent.subscribeTo(newNode, channel);
        }
//        List<ExecutableNodeBase> parentDependants = parent.getExecutableNodeBaseDependents();
//        int idx = parentDependants.indexOf(oldNode);
//        parentDependants.remove(idx);
//        parentDependants.add(idx, newNode);
      }
//      root.cancelSubscriptionTo(oldNode);
    }

    return root;
  }

  // identify the nodes that are 
  // (1) aggregates with scrambled tables and 
  // (2) are not descendants of any other top aggregates.
  static List<AggExecutionNodeBlock> identifyTopAggBlocks(ScrambleMeta scrambleMeta, ExecutableNodeBase root) {
    List<AggExecutionNodeBlock> aggblocks = new ArrayList<>();
//    ScrambleMeta scrambleMeta = root.getPlan().getScrambleMeta();

    if (root instanceof AggExecutionNode) {
      // check if it contains at least one scrambled table.
      if (doesContainScramble(root, scrambleMeta)) {
        AggExecutionNodeBlock block = new AggExecutionNodeBlock(((AggExecutionNode) root).getNamer(), root);
        aggblocks.add(block);
        return aggblocks;
      }
    }
    
    for (ExecutableNodeBase dep : root.getExecutableNodeBaseDependents()) {
      List<AggExecutionNodeBlock> depAggBlocks = identifyTopAggBlocks(scrambleMeta, dep);
      aggblocks.addAll(depAggBlocks);
    }

    return aggblocks;
  }
  
  static boolean doesContainScramble(ExecutableNodeBase node, ScrambleMeta scrambleMeta) {
    SelectQuery query = ((QueryNodeBase) node).getSelectQuery();
    
    // check within the query
    for (AbstractRelation rel : query.getFromList()) {
      if (rel instanceof BaseTable) {
        BaseTable base = (BaseTable) rel;
        String schemaName = base.getSchemaName();
        String tableName = base.getTableName();
        if (scrambleMeta.isScrambled(schemaName, tableName)) {
          return true;
        }
      } else if (rel instanceof JoinTable) {
        for (AbstractRelation r : ((JoinTable) rel).getJoinList()) {
          if (r instanceof BaseTable) {
            BaseTable base = (BaseTable) r;
            String schemaName = base.getSchemaName();
            String tableName = base.getTableName();
            if (scrambleMeta.isScrambled(schemaName, tableName)) {
              return true;
            }
          }
        }
      }
      // SelectQuery is not supposed to be passed.
    }
    
    for (ExecutableNodeBase dep : node.getExecutableNodeBaseDependents()) {
      if (dep instanceof AggExecutionNode) {
        continue;
      }
      if (doesContainScramble(dep, scrambleMeta)) {
        return true;
      }
    }
    return false;
  }

}
