package org.verdictdb.core.querying.ola;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.core.querying.AggExecutionNode;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.QueryNodeBase;
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

  ExecutableNodeBase blockRoot;

  List<ExecutableNodeBase> blockNodes;

  public AggExecutionNodeBlock(TempIdCreator idCreator, ExecutableNodeBase blockRoot) {
    this.idCreator = idCreator;
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
  public ExecutableNodeBase convertToProgressiveAgg(ScrambleMeta scrambleMeta) 
      throws VerdictDBValueException {
    List<ExecutableNodeBase> individualAggNodes = new ArrayList<>();
    List<ExecutableNodeBase> combiners = new ArrayList<>();
    //    ScrambleMeta scrambleMeta = idCreator.getScrambleMeta();

    // first, plan how to perform block aggregation
    // filtering predicates inserted into different scrambled tables are identified.
    List<Pair<ExecutableNodeBase, Triple<String, String, String>>> scrambledNodes = 
        identifyScrambledNodes(scrambleMeta, blockNodes);
    List<Pair<String, String>> scrambles = new ArrayList<>();
    for (Pair<ExecutableNodeBase, Triple<String, String, String>> a : scrambledNodes) {
      String schemaName = a.getRight().getLeft();
      String tableName = a.getRight().getMiddle();
      scrambles.add(Pair.of(schemaName, tableName));
    }
    OlaAggregationMetaData aggMeta = new OlaAggregationMetaData(scrambleMeta, scrambles);

    // second, according to the plan, create individual nodes that perform aggregations.
    for (int i = 0; i < aggMeta.totalBlockAggCount(); i++) {
      // copy and remove the dependency to its parents
      AggExecutionNodeBlock copy = deepcopyExcludingDependentAggregates();
      ExecutableNodeBase aggroot = copy.getBlockRootNode();
      for (ExecutableNodeBase parent : aggroot.getExecutableNodeBaseParents()) {
        parent.cancelSubscriptionTo(aggroot);
      }
      aggroot.clearSubscribers();

      // add extra predicates to restrain each aggregation to particular parts of base tables.
      List<Pair<ExecutableNodeBase, Triple<String, String, String>>> scrambledNodeAndTableName = 
          identifyScrambledNodes(scrambleMeta, copy.getNodesInBlock());

      // TODO: dimension should not be created here
      // simply use OlaAggregationMetaData
      if (scrambles.size()==1) {
        Dimension dimension = new Dimension(scrambles.get(0).getLeft(), scrambles.get(0).getRight(), i, i);
        ((AggExecutionNode) aggroot).getCubes().addAll(Arrays.asList(new HyperTableCube(Arrays.asList(dimension))));
      }
      else {
        int turn = i % scrambles.size();
        int round = i / scrambles.size() + 1;
        List<Dimension> dimensionList = new ArrayList<>();
        for (int j = 0; j<scrambles.size(); j++) {
          int blockCount = scrambleMeta.getAggregationBlockCount(scrambles.get(j).getLeft(), scrambles.get(j).getRight());
          if (turn==j) {
            Dimension d = new Dimension(scrambles.get(j).getLeft(), scrambles.get(j).getRight(),round-1, round-1);
            dimensionList.add(d);
          }
          else {
            Dimension d;
            if (j<turn) {
              d = new Dimension(scrambles.get(j).getLeft(), scrambles.get(j).getRight(), round, blockCount-1);
            }
            else {
              d = new Dimension(scrambles.get(j).getLeft(), scrambles.get(j).getRight(), round - 1, blockCount-1);
            }
            dimensionList.add(d);
          }
        }
        ((AggExecutionNode)aggroot).getCubes().addAll(Arrays.asList(new HyperTableCube(dimensionList)));
      }

      // insert predicates
      for (Pair<ExecutableNodeBase, Triple<String, String, String>> a : scrambledNodeAndTableName) {
        ExecutableNodeBase scrambledNode = a.getLeft();
        String schemaName = a.getRight().getLeft();
        String tableName = a.getRight().getMiddle();
        String aliasName = a.getRight().getRight();
        Pair<Integer, Integer> span = aggMeta.getAggBlockSpanForTable(schemaName, tableName, i);
        String aggblockColumn = scrambleMeta.getAggregationBlockColumn(schemaName, tableName);
        SelectQuery q = ((QueryNodeBase) scrambledNode).getSelectQuery();
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
    for (ExecutableNodeBase n : individualAggNodes) {
      n.clearSubscribers();
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
    ExecutableNodeBase newRoot = AsyncAggExecutionNode.create(idCreator, individualAggNodes, combiners, scrambleMeta);

    return newRoot;
  }

  List<Pair<ExecutableNodeBase, Triple<String, String, String>>> 
  identifyScrambledNodes(ScrambleMeta scrambleMeta, List<ExecutableNodeBase> blockNodes) {

    List<Pair<ExecutableNodeBase, Triple<String, String, String>>> identified = new ArrayList<>();

    for (ExecutableNodeBase node : blockNodes) {
      for (AbstractRelation rel : ((QueryNodeBase) node).getSelectQuery().getFromList()) {
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

  /**
   * Replicas of the group is made. The subscription relationships among the group's nodes are replicated. 
   * The subscription relationships outside the group's nodes are shared. This is for each replicated group 
   * to receive the same information from the downstream operations.
   * 
   * @param root
   * @return
   * @throws VerdictDBValueException 
   */
  public AggExecutionNodeBlock deepcopyExcludingDependentAggregates() throws VerdictDBValueException {
    List<ExecutableNodeBase> newNodes = new ArrayList<>();
    for (ExecutableNodeBase node : blockNodes) {
      ExecutableNodeBase copied = node.deepcopy();
      copied.clearSubscribers();    // this subscription information will be properly reconstructed below.
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
        }

      }
    }

    // compose a return value
    int rootIdx = blockNodes.indexOf(blockRoot);
    return new AggExecutionNodeBlock(idCreator, newNodes.get(rootIdx)); 
  }

}
