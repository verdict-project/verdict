package org.verdictdb.core.execution.ola;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.core.execution.AggExecutionNode;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.execution.QueryExecutionNode;
import org.verdictdb.core.execution.QueryExecutionPlan;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.BaseColumn;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.ConstantColumn;
import org.verdictdb.core.query.JoinTable;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.exception.VerdictDBValueException;

/**
 * Contains the references to the ExecutionNodes that contain scrambled tables. This does not include
 * the scrambled tables in sub aggregate queries.
 * 
 * @author Yongjoo Park
 *
 */
public class AggExecutionNodeBlock {
  
  QueryExecutionPlan plan;
  
  QueryExecutionNode blockRoot;
  
  List<QueryExecutionNode> blockNodes;
  
//  List<QueryExecutionNode> scrambledNodes;    // nodes including scrambled tables

  public AggExecutionNodeBlock(QueryExecutionPlan plan, QueryExecutionNode blockRoot) {
    this.plan = plan;
    this.blockRoot = blockRoot;
    this.blockNodes = getNodesInBlock(blockRoot);
//    this.scrambledNodes = identifyScrambledNodes(blockNodes);
  }
  
  public QueryExecutionNode getBlockRootNode() {
    return blockRoot;
  }
  
  public List<QueryExecutionNode> getNodesInBlock() {
    return blockNodes;
  }
  
  List<QueryExecutionNode> getNodesInBlock(QueryExecutionNode root) {
    List<QueryExecutionNode> nodes = new ArrayList<>();
    nodes.add(root);
    
    for (QueryExecutionNode dep : root.getDependents()) {
      if (dep instanceof AggExecutionNode) {
        continue;
      } else {
        List<QueryExecutionNode> depNodes = getNodesInBlock(dep);
        nodes.addAll(depNodes);
      }
    }
    
    return nodes;
  }
  
  /**
   * Converts the root node and its descendants into the configuration that enables progressive aggregation.
   * 
   * Basically aggregate subqueries are blocking operations while others operations are divided into smaller-
   * scale operations (which involve different portions of data).
   * 
   * @param nodeBlock
   * @return Returns the root of the multiple aggregation nodes (each of which involves different combinations
   * of partitions)
   * @throws VerdictDBValueException 
   */
  public QueryExecutionNode convertToProgressiveAgg () throws VerdictDBValueException {
    List<QueryExecutionNode> individualAggNodes = new ArrayList<>();
    List<QueryExecutionNode> combiners = new ArrayList<>();
    ScrambleMeta scrambleMeta = plan.getScrambleMeta();
    
    // first, plan how to perform block aggregation
    // filtering predicates inserted into different scrambled tables are identified.
    List<Pair<QueryExecutionNode, Triple<String, String, String>>> scrambledNodes = 
        identifyScrambledNodes(scrambleMeta, blockNodes);
    List<Pair<String, String>> scrambles = new ArrayList<>();
    for (Pair<QueryExecutionNode, Triple<String, String, String>> a : scrambledNodes) {
      String schemaName = a.getRight().getLeft();
      String tableName = a.getRight().getMiddle();
      scrambles.add(Pair.of(schemaName, tableName));
    }
    AggBlockMeta aggMeta = new AggBlockMeta(scrambleMeta, scrambles);
    
    // second, according to the plan, create individual nodes that perform aggregations.
    for (int i = 0; i < aggMeta.totalBlockAggCount(); i++) {
      AggExecutionNodeBlock copy = deepcopyExcludingDependentAggregates();
      List<Pair<QueryExecutionNode, Triple<String, String, String>>> scrambledNodeAndTableName = 
          identifyScrambledNodes(scrambleMeta, copy.getNodesInBlock());
      
      // add extra predicates to restrain each aggregation to particular parts of base tables.
      for (Pair<QueryExecutionNode, Triple<String, String, String>> a : scrambledNodeAndTableName) {
        QueryExecutionNode scrambledNode = a.getLeft();
        String schemaName = a.getRight().getLeft();
        String tableName = a.getRight().getMiddle();
        String aliasName = a.getRight().getRight();
        Pair<Integer, Integer> span = aggMeta.getAggBlockSpanForTable(schemaName, tableName, i);
        String aggblockColumn = scrambleMeta.getAggregationBlockColumn(schemaName, tableName);
        SelectQuery q = (SelectQuery) scrambledNode.getSelectQuery();
//        String aliasName = findAliasFor(schemaName, tableName, q.getFromList());
        if (aliasName == null) {
          throw new VerdictDBValueException(String.format("The alias name for the table (%s, %s) is not found.", schemaName, tableName));
        }
        
        int left = span.getLeft();
        int right = span.getRight();
        if (left == right) {
          q.addFilterByAnd(ColumnOp.equal(new BaseColumn(aliasName, aggblockColumn), ConstantColumn.valueOf(left)));
        } else {
          q.addFilterByAnd(ColumnOp.greaterequal(
              new BaseColumn(aliasName, aggblockColumn),
              ConstantColumn.valueOf(left)));
          q.addFilterByAnd(ColumnOp.lessequal(
              new BaseColumn(aliasName, aggblockColumn),
              ConstantColumn.valueOf(right)));
        }
      }
      
      individualAggNodes.add(copy.getBlockRootNode());
    }
    
    // third, stack combiners
    // clear existing broadcasting queues of individual agg nodes
    for (QueryExecutionNode n : individualAggNodes) {
      n.clearBroadcastingQueues();
    }
    for (int i = 1; i < aggMeta.totalBlockAggCount(); i++) {
      AggCombinerExecutionNode combiner;
      if (i == 1) {
        combiner = AggCombinerExecutionNode.create(
            plan,
            individualAggNodes.get(0), 
            individualAggNodes.get(1));
      } else {
        combiner = AggCombinerExecutionNode.create(
            plan,
            combiners.get(i-2), 
            individualAggNodes.get(i));
      }
      combiners.add(combiner);
    }
    
    // fourth, re-link the listening queue for the new AsyncAggNode
    QueryExecutionNode newRoot = AsyncAggExecutionNode.create(plan, individualAggNodes, combiners);
    
    // root to upstream
    List<ExecutionTokenQueue> broadcastingQueue = blockRoot.getBroadcastingQueues();
    for (ExecutionTokenQueue queue : broadcastingQueue) {
      newRoot.addBroadcastingQueue(queue);
    }
    
    return newRoot;
  }
  
  List<Pair<QueryExecutionNode, Triple<String, String, String>>> 
  identifyScrambledNodes(ScrambleMeta scrambleMeta, List<QueryExecutionNode> blockNodes) {
    List<Pair<QueryExecutionNode, Triple<String, String, String>>> identified = new ArrayList<>();
    
    for (QueryExecutionNode node : blockNodes) {
      for (AbstractRelation rel : node.getSelectQuery().getFromList()) {
        if (rel instanceof BaseTable) {
          BaseTable base = (BaseTable) rel;
          if (scrambleMeta.isScrambled(base.getSchemaName(), base.getTableName())) {
            identified.add(Pair.of(
                node, 
                Triple.of(
                    base.getSchemaName(), 
                    base.getTableName(),
                    base.getAliasName().get())));
          }
        } 
        else if (rel instanceof JoinTable) {
          for (AbstractRelation r : ((JoinTable) rel).getJoinList()) {
            if (r instanceof BaseTable) {
              BaseTable base = (BaseTable) r;
              if (scrambleMeta.isScrambled(base.getSchemaName(), base.getTableName())) {
                identified.add(
                    Pair.of(
                        node, 
                        Triple.of(
                            base.getSchemaName(), 
                            base.getTableName(),
                            base.getAliasName().get())));
              }
            }
          }
        }
      }
    }
    
    return identified;
  }

  String findAliasFor(String schemaName, String tableName, List<AbstractRelation> fromList) {
    for (AbstractRelation rel : fromList) {
      if (rel instanceof BaseTable) {
        BaseTable base = (BaseTable) rel;
        if (schemaName.equals(base.getSchemaName()) && tableName.equals(base.getTableName())) {
          return base.getAliasName().get();
        }
      } 
      else if (rel instanceof JoinTable) {
        for (AbstractRelation r : ((JoinTable) rel).getJoinList()) {
          if (r instanceof BaseTable) {
            BaseTable base = (BaseTable) rel;
            if (schemaName.equals(base.getSchemaName()) && tableName.equals(base.getTableName())) {
              return base.getAliasName().get();
            }
          }
        }
      }
    }
    return null;
  }

//  /**
//   * 
//   * @param root
//   * @return Pair of node and (schemaName, tableName) of the scrambled table. If there are multiple scrambled
//   * tables in a single query, return the same node with different schemaName.tableName.
//   */
//  List<Pair<QueryExecutionNode, Pair<String, String>>> identifyNodesWithScrambles(
//      ScrambleMeta scrambleMeta, 
//      QueryExecutionNode root) {
//    List<Pair<QueryExecutionNode, Pair<String, String>>> identified = new ArrayList<>();
//    if (root instanceof AggExecutionNode) {
//      for (AbstractRelation rel : root.getSelectQuery().getFromList()) {
//        if (!(rel instanceof BaseTable)) {
//          continue;
//        } else {
//          BaseTable base = (BaseTable) rel;
//          if (scrambleMeta.isScrambled(base.getSchemaName(), base.getTableName())) {
//            identified.add(Pair.of(root, Pair.of(base.getSchemaName(), base.getTableName())));
//          }
//        }
//      }
//    }
//    
//    // repeat the process for dependents
//    for (QueryExecutionNode dep : root.getDependents()) {
//      if (dep instanceof AggExecutionNode) {
//        continue;
//      } else {
//        List<Pair<QueryExecutionNode, Pair<String, String>>> identifiedDep = 
//            identifyNodesWithScrambles(scrambleMeta, dep);
//        identified.addAll(identifiedDep);
//      }
//    }
//    
//    return identified;
//  }

  /**
   * Replicas of the group is made. The token queues among the group's nodes are replicated. The token queues
   * outside the group's nodes are shared. This is for each replicated group to receive the same information
   * from the downstream operations.
   * 
   * @param root
   * @return
   */
  public AggExecutionNodeBlock deepcopyExcludingDependentAggregates() {
    List<QueryExecutionNode> newNodes = new ArrayList<>();
    for (QueryExecutionNode node : blockNodes) {
      newNodes.add(node.deepcopy());
    }
    
    // reconstruct the dependency relationship
    // if there are nodes (in this block) on which other nodes depend on, we restore that dependency
    // relationship.
    for (int i = 0; i < newNodes.size(); i++) {
      QueryExecutionNode newNode = newNodes.get(i);
      QueryExecutionNode oldNode = blockNodes.get(i);
      for (QueryExecutionNode dep : oldNode.getDependents()) {
        int idx = blockNodes.indexOf(dep);
        if (idx >= 0) {
          newNode.getDependents().set(idx, newNodes.get(idx));
        }
      }
    }
    
    // reconstruct listening and broadcasting queues
    // the existing broadcasting nodes are all replaced with new ones. the listening nodes are updated
    // appropriately.
    for (int i = 0; i < newNodes.size(); i++) {
      QueryExecutionNode newNode = newNodes.get(i);
      QueryExecutionNode oldNode = blockNodes.get(i);
      List<ExecutionTokenQueue> listeningQueues = oldNode.getListeningQueues();
      for (int listeningQueueIdx = 0;  listeningQueueIdx < listeningQueues.size(); listeningQueueIdx++) {
        ExecutionTokenQueue listeningQueue = listeningQueues.get(listeningQueueIdx);
        
        for (int nodeIdx = 0; nodeIdx < blockNodes.size(); nodeIdx++) {
          QueryExecutionNode node = blockNodes.get(nodeIdx);
          int broadcastQueueIdx = node.getBroadcastingQueues().indexOf(listeningQueue);
          if (broadcastQueueIdx >= 0) {
            ExecutionTokenQueue newQueue = newNode.generateListeningQueue();
            newNode.getListeningQueues().set(listeningQueueIdx, newQueue);       // insert in the middle
            newNode.getListeningQueues().remove(newNode.getListeningQueues().size()-1);   // remove last
            newNodes.get(nodeIdx).getBroadcastingQueues().set(broadcastQueueIdx, newQueue);
          }
        }
      }
    }
    
    // compose the return value
    int rootIdx = blockNodes.indexOf(blockRoot);
    return new AggExecutionNodeBlock(plan, newNodes.get(rootIdx));
  }

}
