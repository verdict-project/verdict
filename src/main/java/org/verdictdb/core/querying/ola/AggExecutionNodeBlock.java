/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.core.querying.ola;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.core.querying.AggExecutionNode;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.exception.VerdictDBValueException;

/**
 * Contains the references to the ExecutionNodes that contain scrambled tables. This does not
 * include the scrambled tables in sub aggregate queries.
 *
 * @author Yongjoo Park
 */
public class AggExecutionNodeBlock {

  ExecutableNodeBase blockRoot;

  List<ExecutableNodeBase> blockNodes;

  public AggExecutionNodeBlock(ExecutableNodeBase blockRoot) {
    //    this.idCreator = idCreator;
    this.blockRoot = blockRoot;
    this.blockNodes = getNodesInBlock(blockRoot);
  }

  public ExecutableNodeBase getBlockRootNode() {
    return blockRoot;
  }

  public List<ExecutableNodeBase> getNodesInBlock() {
    return blockNodes;
  }

  List<ExecutableNodeBase> getNodesInBlock(ExecutableNodeBase root) {
    List<ExecutableNodeBase> nodes = new ArrayList<>();
    nodes.add((QueryNodeBase) root);

    for (ExecutableNodeBase dep : root.getExecutableNodeBaseDependents()) {
      if (dep instanceof AggExecutionNode) {
        continue;
      } else {
        List<ExecutableNodeBase> depNodes = getNodesInBlock(dep);
        nodes.addAll(depNodes);
      }
    }

    return nodes;
  }

  //  /**
  //   * Converts the root node and its descendants into the configuration that enables progressive
  // aggregation.
  //   *
  //   * Basically aggregate subqueries are blocking operations while others operations are divided
  // into smaller-
  //   * scale operations (which involve different portions of data).
  //   *
  //   * @param nodeBlock
  //   * @return Returns the root of the multiple aggregation nodes (each of which involves
  // different combinations
  //   * of partitions)
  //   * @throws VerdictDBValueException
  //   */
  //  public ExecutableNodeBase convertToProgressiveAgg(ScrambleMetaSet scrambleMeta)
  //      throws VerdictDBValueException {
  //    List<ExecutableNodeBase> individualAggNodes = new ArrayList<>();
  //    List<ExecutableNodeBase> combiners = new ArrayList<>();
  //    //    ScrambleMeta scrambleMeta = idCreator.getScrambleMeta();
  //
  //    // First, plan how to perform block aggregation
  //    // filtering predicates that must inserted into different scrambled tables are identified.
  //    List<Pair<ExecutableNodeBase, Triple<String, String, String>>> scrambledNodes =
  //        identifyScrambledNodes(scrambleMeta, blockNodes);
  //    List<Pair<String, String>> scrambles = new ArrayList<>();
  //    for (Pair<ExecutableNodeBase, Triple<String, String, String>> a : scrambledNodes) {
  //      String schemaName = a.getRight().getLeft();
  //      String tableName = a.getRight().getMiddle();
  //      scrambles.add(Pair.of(schemaName, tableName));
  //    }
  //    OlaAggregationPlan aggPlan = new OlaAggregationPlan(scrambleMeta, scrambles);
  //    List<Pair<ExecutableNodeBase, ExecutableNodeBase>> oldSubscriptionInformation =
  //        new ArrayList<>();
  //
  //    // Second, according to the plan, create individual nodes that perform aggregations.
  //    for (int i = 0; i < aggPlan.totalBlockAggCount(); i++) {
  //
  //      // copy and remove the dependency to its parents
  //      oldSubscriptionInformation.clear();
  //      AggExecutionNodeBlock copy =
  // deepcopyExcludingDependentAggregates(oldSubscriptionInformation);
  //      ExecutableNodeBase aggroot = copy.getBlockRootNode();
  //      for (ExecutableNodeBase parent : aggroot.getExecutableNodeBaseParents()) {
  //        parent.cancelSubscriptionTo(aggroot);   // not sure if this is required, but do anyway
  //      }
  //      aggroot.cancelSubscriptionsFromAllSubscribers();   // subscription will be reconstructed
  // later.
  //
  //      // Add extra predicates to restrain each aggregation to particular parts of base tables.
  //      List<Pair<ExecutableNodeBase, Triple<String, String, String>>> scrambledNodeAndTableName =
  //          identifyScrambledNodes(scrambleMeta, copy.getNodesInBlock());
  //
  //      // Assign hyper table cube to the block
  //      ((AggExecutionNode)aggroot).getMeta().setCubes(Arrays.asList(aggPlan.cubes.get(i)));
  //
  //      // Search for agg column
  //      // Rewrite individual aggregate node so that it only select basic aggregate column and
  // non-aggregate column
  //      List<SelectItem> newSelectlist =
  // rewriteSelectlistWithBasicAgg(((AggExecutionNode)aggroot).getSelectQuery(),
  // ((AggExecutionNode)aggroot).getMeta());
  //
  //      // Add a tier column and a group attribute if the from list has multiple tier table
  //      addTierColumn(((AggExecutionNode)aggroot).getSelectQuery(), newSelectlist, scrambleMeta);
  //
  //      ((AggExecutionNode)aggroot).getSelectQuery().clearSelectList();
  //      ((AggExecutionNode)aggroot).getSelectQuery().getSelectList().addAll(newSelectlist);
  //
  //      aggColumnIdentiferNum = 0;
  //
  //      // Insert predicates into individual aggregation nodes
  //      for (Pair<ExecutableNodeBase, Triple<String, String, String>> a :
  // scrambledNodeAndTableName) {
  //        ExecutableNodeBase scrambledNode = a.getLeft();
  //        String schemaName = a.getRight().getLeft();
  //        String tableName = a.getRight().getMiddle();
  //        String aliasName = a.getRight().getRight();
  //        Pair<Integer, Integer> span = aggPlan.getAggBlockSpanForTable(schemaName, tableName, i);
  //        String aggblockColumn = scrambleMeta.getAggregationBlockColumn(schemaName, tableName);
  //        SelectQuery q = ((QueryNodeBase) scrambledNode).getSelectQuery();
  //        //        String aliasName = findAliasFor(schemaName, tableName, q.getFromList());
  //        if (aliasName == null) {
  //          throw new VerdictDBValueException(String.format("The alias name for the table (%s, %s)
  // is not found.", schemaName, tableName));
  //        }
  //
  //        int left = span.getLeft();
  //        int right = span.getRight();
  //        if (left == right) {
  //          q.addFilterByAnd(ColumnOp.equal(new BaseColumn(aliasName, aggblockColumn),
  // ConstantColumn.valueOf(left)));
  //        } else {
  //          q.addFilterByAnd(ColumnOp.greaterequal(
  //              new BaseColumn(aliasName, aggblockColumn),
  //              ConstantColumn.valueOf(left)));
  //          q.addFilterByAnd(ColumnOp.lessequal(
  //              new BaseColumn(aliasName, aggblockColumn),
  //              ConstantColumn.valueOf(right)));
  //        }
  //      }
  //
  //      individualAggNodes.add(aggroot);
  //    }
  //
  //    // Third, stack combiners
  //    // clear existing broadcasting queues of individual agg nodes
  //    for (ExecutableNodeBase n : individualAggNodes) {
  //      n.cancelSubscriptionsFromAllSubscribers();
  //    }
  //    for (int i = 1; i < aggPlan.totalBlockAggCount(); i++) {
  //      AggCombinerExecutionNode combiner;
  //      if (i == 1) {
  //        combiner = AggCombinerExecutionNode.create(
  //            idCreator,
  //            individualAggNodes.get(0),
  //            individualAggNodes.get(1));
  //      } else {
  //        combiner = AggCombinerExecutionNode.create(
  //            idCreator,
  //            combiners.get(i-2),
  //            individualAggNodes.get(i));
  //      }
  //      combiners.add(combiner);
  //    }
  //
  //    // Fourth, re-link the subscription relationship for the new AsyncAggNode
  //    ExecutableNodeBase newRoot = AsyncAggExecutionNode.create(idCreator, individualAggNodes,
  // combiners, scrambleMeta);
  //
  //    // Finally remove the old subscription information: old copied node -> still used old node
  //    for (Pair<ExecutableNodeBase, ExecutableNodeBase> parentToSource :
  // oldSubscriptionInformation) {
  //      ExecutableNodeBase subscriber = parentToSource.getLeft();
  //      ExecutableNodeBase source = parentToSource.getRight();
  //      subscriber.cancelSubscriptionTo(source);
  //    }
  //
  //    return newRoot;
  //  }

  /**
   * @param scrambleMeta Information about what tables have been scrambled.
   * @param blockNodes
   * @return ExecutableNodeBase is the reference to the scrambled base table. The triple is (schema,
   *     table, alias) of scrambled tables.
   */
  List<Pair<ExecutableNodeBase, Triple<String, String, String>>> identifyScrambledNodes(
      ScrambleMetaSet scrambleMeta, List<ExecutableNodeBase> blockNodes) {

    List<Pair<ExecutableNodeBase, Triple<String, String, String>>> identified = new ArrayList<>();

    for (ExecutableNodeBase node : blockNodes) {
      for (AbstractRelation rel : ((QueryNodeBase) node).getSelectQuery().getFromList()) {
        if (rel instanceof BaseTable) {
          BaseTable base = (BaseTable) rel;
          if (scrambleMeta.isScrambled(base.getSchemaName(), base.getTableName())) {
            identified.add(
                Pair.of(
                    node,
                    Triple.of(
                        base.getSchemaName(), base.getTableName(), base.getAliasName().get())));
          }
        } else if (rel instanceof JoinTable) {
          for (AbstractRelation r : ((JoinTable) rel).getJoinList()) {
            if (r instanceof BaseTable) {
              BaseTable base = (BaseTable) r;
              if (scrambleMeta.isScrambled(base.getSchemaName(), base.getTableName())) {
                identified.add(
                    Pair.of(
                        node,
                        Triple.of(
                            base.getSchemaName(), base.getTableName(), base.getAliasName().get())));
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
      } else if (rel instanceof JoinTable) {
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

  /**
   * Replicas of the group is made. The subscription relationships among the group's nodes are
   * replicated. The subscription relationships outside the group's nodes are shared. This is for
   * each replicated group to receive the same information from the downstream operations.
   *
   * @param root
   * @param oldSubscriptionInformation Pairs of (old parent, old source). After all deepcopying is
   *     finished, this old subscription can be cancelled.
   * @return
   * @throws VerdictDBValueException
   */
  public AggExecutionNodeBlock deepcopyExcludingDependentAggregates(
      List<Pair<ExecutableNodeBase, ExecutableNodeBase>> oldSubscriptionInformation)
      throws VerdictDBValueException {
    List<ExecutableNodeBase> newNodes = new ArrayList<>();
    for (ExecutableNodeBase node : blockNodes) {
      ExecutableNodeBase copied = node.deepcopy();
      // this subscription information will be properly reconstructed below.
      copied.cancelSubscriptionsFromAllSubscribers(); 
      newNodes.add(copied);
    }

    // reconstruct dependency relationships
    for (int i = 0; i < newNodes.size(); i++) {
      ExecutableNodeBase newNode = newNodes.get(i);
      ExecutableNodeBase oldNode = blockNodes.get(i);

      for (int j = 0; j < oldNode.getSources().size(); j++) {
        Pair<ExecutableNodeBase, Integer> source = oldNode.getSourcesAndChannels().get(j);
        int idx = blockNodes.indexOf(source.getLeft());

        // at this moment, newNode still has old source information, so we remove it.
        newNode.cancelSubscriptionTo(source.getLeft());

        if (idx >= 0) {
          // internal dependency relationships
          newNode.subscribeTo(newNodes.get(idx), source.getRight());
        } else {
          // external dependency relationships
          newNode.subscribeTo(source.getLeft(), source.getRight());

          // store old subscription information for reference
          oldSubscriptionInformation.add(Pair.of(oldNode, source.getLeft()));
        }
      }
    }

    // compose a return value
    int rootIdx = blockNodes.indexOf(blockRoot);
    return new AggExecutionNodeBlock(newNodes.get(rootIdx));
  }

  public List<ExecutableNodeBase> getLeafNodes() {
    List<ExecutableNodeBase> leafNodes = new ArrayList<>();
    for (ExecutableNodeBase node : blockNodes) {
      List<ExecutableNodeBase> sources = node.getSources();
      boolean allSourcesExternal = true;
      for (ExecutableNodeBase source : sources) {
        if (blockNodes.contains(source)) {
          allSourcesExternal = false;
        }
      }
      
      if (allSourcesExternal) {
        leafNodes.add(node);
      }
    }
    
    return leafNodes;
  }
}
