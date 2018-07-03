package org.verdictdb.core.querying.ola;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.querying.AggExecutionNode;
import org.verdictdb.core.querying.BaseQueryNode;
import org.verdictdb.core.querying.TempIdCreator;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBValueException;

/**
 * Contains the references to the ExecutionNodes that contain scrambled tables. This does not include
 * the scrambled tables in sub aggregate queries.
 * 
 * @author Yongjoo Park
 *
 */
public class AggExecutionNodeBlock {
  
  TempIdCreator idCreator;
  
  BaseQueryNode blockRoot;
  
  List<BaseQueryNode> blockNodes;
  
//  List<QueryExecutionNode> scrambledNodes;    // nodes including scrambled tables

  public AggExecutionNodeBlock(TempIdCreator idCreator, BaseQueryNode blockRoot) {
    this.idCreator = idCreator;
    this.blockRoot = blockRoot;
    this.blockNodes = getNodesInBlock(blockRoot);
//    this.scrambledNodes = identifyScrambledNodes(blockNodes);
  }
  
  public BaseQueryNode getBlockRootNode() {
    return blockRoot;
  }
  
  public List<BaseQueryNode> getNodesInBlock() {
    return blockNodes;
  }
  
  List<BaseQueryNode> getNodesInBlock(BaseQueryNode root) {
    List<BaseQueryNode> nodes = new ArrayList<>();
    nodes.add(root);
    
    for (BaseQueryNode dep : root.getDependents()) {
      if (dep instanceof AggExecutionNode) {
        continue;
      } else {
        List<BaseQueryNode> depNodes = getNodesInBlock(dep);
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
  public BaseQueryNode convertToProgressiveAgg (ScrambleMeta scrambleMeta) 
      throws VerdictDBValueException {
    List<BaseQueryNode> individualAggNodes = new ArrayList<>();
    List<BaseQueryNode> combiners = new ArrayList<>();
//    ScrambleMeta scrambleMeta = idCreator.getScrambleMeta();
    
    // first, plan how to perform block aggregation
    // filtering predicates inserted into different scrambled tables are identified.
    List<Pair<BaseQueryNode, Triple<String, String, String>>> scrambledNodes = 
        identifyScrambledNodes(scrambleMeta, blockNodes);
    List<Pair<String, String>> scrambles = new ArrayList<>();
    for (Pair<BaseQueryNode, Triple<String, String, String>> a : scrambledNodes) {
      String schemaName = a.getRight().getLeft();
      String tableName = a.getRight().getMiddle();
      scrambles.add(Pair.of(schemaName, tableName));
    }
    AggBlockMeta aggMeta = new AggBlockMeta(scrambleMeta, scrambles);
    
    // second, according to the plan, create individual nodes that perform aggregations.
    for (int i = 0; i < aggMeta.totalBlockAggCount(); i++) {
      // copy and remove the dependency to its parents
      AggExecutionNodeBlock copy = deepcopyExcludingDependentAggregates();
      BaseQueryNode aggroot = copy.getBlockRootNode();
      for (BaseQueryNode parent : aggroot.getParents()) {
        parent.getDependents().remove(aggroot);
      }
      aggroot.getParents().clear();
      
      List<Pair<BaseQueryNode, Triple<String, String, String>>> scrambledNodeAndTableName = 
          identifyScrambledNodes(scrambleMeta, copy.getNodesInBlock());
      
      // add extra predicates to restrain each aggregation to particular parts of base tables.
      // Move original AggExecutionNode.executeNode() method here
      // Assume only one scramble table in the query
      Dimension dimension = new Dimension(scrambles.get(0).getLeft(), scrambles.get(0).getRight(), i, i);
      ((AggExecutionNode)aggroot).getCubes().addAll(Arrays.asList(new HyperTableCube(Arrays.asList(dimension))));

      for (Pair<BaseQueryNode, Triple<String, String, String>> a : scrambledNodeAndTableName) {
        BaseQueryNode scrambledNode = a.getLeft();
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
      
      individualAggNodes.add(aggroot);
    }
    
    // third, stack combiners
    // clear existing broadcasting queues of individual agg nodes
    for (BaseQueryNode n : individualAggNodes) {
      n.getParents().clear();
      n.clearBroadcastingQueues();
    }
    for (int i = 1; i < aggMeta.totalBlockAggCount(); i++) {
      AggCombinerExecutionNode combiner;
      if (i == 1) {
        combiner = AggCombinerExecutionNode.create(
            idCreator,
            individualAggNodes.get(0), 
            individualAggNodes.get(1));
      } else {
        combiner = AggCombinerExecutionNode.create(
            idCreator,
            combiners.get(i-2), 
            individualAggNodes.get(i));
      }
      combiners.add(combiner);
    }
    
    // fourth, re-link the listening queue for the new AsyncAggNode
    BaseQueryNode newRoot = AsyncAggExecutionNode.create(idCreator, individualAggNodes, combiners, scrambleMeta);
    
    // root to upstream
    List<ExecutionTokenQueue> broadcastingQueue = blockRoot.getBroadcastingQueues();
    for (ExecutionTokenQueue queue : broadcastingQueue) {
      newRoot.addBroadcastingQueue(queue);
    }
    
    return newRoot;
  }
  
  List<Pair<BaseQueryNode, Triple<String, String, String>>> 
  identifyScrambledNodes(ScrambleMeta scrambleMeta, List<BaseQueryNode> blockNodes) {
    List<Pair<BaseQueryNode, Triple<String, String, String>>> identified = new ArrayList<>();
    
    for (BaseQueryNode node : blockNodes) {
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
   * @throws VerdictDBValueException 
   */
  public AggExecutionNodeBlock deepcopyExcludingDependentAggregates() throws VerdictDBValueException {
    List<BaseQueryNode> newNodes = new ArrayList<>();
    for (BaseQueryNode node : blockNodes) {
      newNodes.add(node.deepcopy());
    }
    
    // reconstruct the dependency relationship
    // if there are nodes (in this block) on which other nodes depend on, we restore that dependency
    // relationship.
    for (int i = 0; i < newNodes.size(); i++) {
      BaseQueryNode newNode = newNodes.get(i);
      BaseQueryNode oldNode = blockNodes.get(i);
      for (int j = 0; j < oldNode.getDependents().size(); j++) {
        BaseQueryNode dep = oldNode.getDependent(j);
        int idx = blockNodes.indexOf(dep);
        if (idx >= 0) {
          newNode.getDependents().set(j, newNodes.get(idx));
        }
      }
    }
    
    // reconstruct listening and broadcasting queues
    // the existing broadcasting nodes are all replaced with new ones. 
    // the listening nodes of leaves are duplicated.
    List<BaseQueryNode> outsideDependents = new ArrayList<>();
    for (BaseQueryNode node : blockNodes) {
      for (BaseQueryNode dep : node.getDependents()) {
        if (!blockNodes.contains(dep)) {
          outsideDependents.add(dep);
        }
      }
    }
    
    for (int i = 0; i < newNodes.size(); i++) {
      BaseQueryNode newNode = newNodes.get(i);
      BaseQueryNode oldNode = blockNodes.get(i);
      List<ExecutionTokenQueue> listeningQueues = oldNode.getListeningQueues();
      
      // for each listening queue of an existing node
      for (int listeningQueueIdx = 0;  listeningQueueIdx < listeningQueues.size(); listeningQueueIdx++) {
        ExecutionTokenQueue listeningQueue = listeningQueues.get(listeningQueueIdx);
        
        // find the nodes that broadcast to this listening queue within this block (intra-block broadcasting)
        for (int nodeIdx = 0; nodeIdx < blockNodes.size(); nodeIdx++) {
          BaseQueryNode node = blockNodes.get(nodeIdx);
          int broadcastQueueIdx = node.getBroadcastingQueues().indexOf(listeningQueue);
          
          if (broadcastQueueIdx >= 0) {
            ExecutionTokenQueue newQueue = newNode.generateReplacementListeningQueue(listeningQueueIdx);
            newNodes.get(nodeIdx).getBroadcastingQueues().set(broadcastQueueIdx, newQueue);
          }
        }
        
        // find the dependents that broadcast to this listening queue outside this block
        for (int nodeIdx = 0; nodeIdx < outsideDependents.size(); nodeIdx++) {
          BaseQueryNode dep = outsideDependents.get(nodeIdx);
          if (dep.getBroadcastingQueues().contains(listeningQueue)) {
            ExecutionTokenQueue newQueue = newNode.generateReplacementListeningQueue(listeningQueueIdx);
            dep.addBroadcastingQueue(newQueue);
          }
        }
      }
    }
    
    // compose the return value
    int rootIdx = blockNodes.indexOf(blockRoot);
    return new AggExecutionNodeBlock(idCreator, newNodes.get(rootIdx));
  }

}
