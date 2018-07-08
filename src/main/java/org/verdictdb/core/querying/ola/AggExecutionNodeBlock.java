package org.verdictdb.core.querying.ola;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.core.querying.AggExecutionNode;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.IdCreator;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasReference;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBValueException;

/**
 * Contains the references to the ExecutionNodes that contain scrambled tables. This does not include
 * the scrambled tables in sub aggregate queries.
 * 
 * @author Yongjoo Park
 *
 */
public class AggExecutionNodeBlock {

  IdCreator idCreator;

  ExecutableNodeBase blockRoot;

  List<ExecutableNodeBase> blockNodes;

  int aggColumnIdentiferNum = 0;

  int verdictdbTierIndentiferNum = 0;

  public AggExecutionNodeBlock(IdCreator idCreator, ExecutableNodeBase blockRoot) {
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
    OlaAggregationPlan aggPlan = new OlaAggregationPlan(scrambleMeta, scrambles);

    // second, according to the plan, create individual nodes that perform aggregations.
    for (int i = 0; i < aggPlan.totalBlockAggCount(); i++) {
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

      // assign hyper table cube to the block
      ((AggExecutionNode)aggroot).getMeta().setCubes(Arrays.asList(aggPlan.cubes.get(i)));

      // Search for agg column
      // rewrite individual aggregate node so that it only select basic aggregate column and non-aggregate column
      List<SelectItem> newSelectlist = rewriteSelectlistWithBasicAgg(((AggExecutionNode)aggroot).getSelectQuery(),  ((AggExecutionNode)aggroot).getMeta());

      // add tier column and group attribute if from list has multiple tier table
      addTierColumn(((AggExecutionNode)aggroot).getSelectQuery(), newSelectlist, scrambleMeta);

      ((AggExecutionNode)aggroot).getSelectQuery().clearSelectList();
      ((AggExecutionNode)aggroot).getSelectQuery().getSelectList().addAll(newSelectlist);

      aggColumnIdentiferNum = 0;

      // insert predicates
      for (Pair<ExecutableNodeBase, Triple<String, String, String>> a : scrambledNodeAndTableName) {
        ExecutableNodeBase scrambledNode = a.getLeft();
        String schemaName = a.getRight().getLeft();
        String tableName = a.getRight().getMiddle();
        String aliasName = a.getRight().getRight();
        Pair<Integer, Integer> span = aggPlan.getAggBlockSpanForTable(schemaName, tableName, i);
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
    for (int i = 1; i < aggPlan.totalBlockAggCount(); i++) {
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


  // judge the aggregate column
  public static List<ColumnOp> getAggregateColumn(UnnamedColumn sel) {
    List<SelectItem> itemToCheck = new ArrayList<>();
    itemToCheck.add(sel);
    List<ColumnOp> columnOps = new ArrayList<>();
    while (!itemToCheck.isEmpty()) {
      SelectItem s = itemToCheck.get(0);
      itemToCheck.remove(0);
      if (s instanceof ColumnOp) {
        if (((ColumnOp) s).getOpType().equals("count") || ((ColumnOp) s).getOpType().equals("sum") || ((ColumnOp) s).getOpType().equals("avg")
            ||((ColumnOp) s).getOpType().equals("max")||((ColumnOp) s).getOpType().equals("min")) {
          columnOps.add((ColumnOp) s);
        }
        else itemToCheck.addAll(((ColumnOp) s).getOperands());
      }
    }
    return columnOps;
  }

  List<SelectItem> rewriteSelectlistWithBasicAgg(SelectQuery query, AggMeta meta) {
    List<SelectItem> selectList = query.getSelectList();
    List<String> aggColumnAlias = new ArrayList<>();
    HashMap<String, String> maxminAlias = new HashMap<>();
    List<SelectItem> newSelectlist = new ArrayList<>();
    meta.setOriginalSelectList(selectList);
    for (SelectItem selectItem : selectList) {
      if (selectItem instanceof AliasedColumn) {
        List<ColumnOp> columnOps = getAggregateColumn(((AliasedColumn) selectItem).getColumn());
        // If it contains agg columns
        if (!columnOps.isEmpty()) {
          meta.getAggColumn().put(selectItem, columnOps);
          for (ColumnOp col:columnOps) {
            if (col.getOpType().equals("avg")) {
              if (!meta.getAggColumnAggAliasPair().containsKey(
                  new ImmutablePair<>("sum", col.getOperand(0)))) {
                ColumnOp col1 = new ColumnOp("sum", col.getOperand(0));
                newSelectlist.add(new AliasedColumn(col1, "agg"+aggColumnIdentiferNum));
                meta.getAggColumnAggAliasPair().put(
                    new ImmutablePair<>("sum", col1.getOperand(0)), "agg"+aggColumnIdentiferNum);
                aggColumnAlias.add("agg"+aggColumnIdentiferNum++);
              }
              if (!meta.getAggColumnAggAliasPair().containsKey(
                  new ImmutablePair<>("count", (UnnamedColumn) new AsteriskColumn()))) {
                ColumnOp col2 = new ColumnOp("count", new AsteriskColumn());
                newSelectlist.add(new AliasedColumn(col2, "agg"+aggColumnIdentiferNum));
                meta.getAggColumnAggAliasPair().put(
                    new ImmutablePair<>("count", (UnnamedColumn) new AsteriskColumn()), "agg"+aggColumnIdentiferNum);
                aggColumnAlias.add("agg"+aggColumnIdentiferNum++);
              }
            } else if (col.getOpType().equals("count") || col.getOpType().equals("sum")){
              if (col.getOpType().equals("count") && !meta.getAggColumnAggAliasPair().containsKey(
                  new ImmutablePair<>("count", (UnnamedColumn)(new AsteriskColumn())))) {
                ColumnOp col1 = new ColumnOp(col.getOpType());
                newSelectlist.add(new AliasedColumn(col1, "agg"+aggColumnIdentiferNum));
                meta.getAggColumnAggAliasPair().put(
                    new ImmutablePair<>(col.getOpType(), (UnnamedColumn)new AsteriskColumn()), "agg"+aggColumnIdentiferNum);
                aggColumnAlias.add("agg"+aggColumnIdentiferNum++);
              }
              else if (!meta.getAggColumnAggAliasPair().containsKey(
                  new ImmutablePair<>(col.getOpType(), col.getOperand(0)))) {
                ColumnOp col1 = new ColumnOp(col.getOpType(), col.getOperand(0));
                newSelectlist.add(new AliasedColumn(col1, "agg"+aggColumnIdentiferNum));
                meta.getAggColumnAggAliasPair().put(
                    new ImmutablePair<>(col.getOpType(), col1.getOperand(0)), "agg"+aggColumnIdentiferNum);
                aggColumnAlias.add("agg"+aggColumnIdentiferNum++);
              }
            } else if (col.getOpType().equals("max") || col.getOpType().equals("min")) {
              ColumnOp col1 = new ColumnOp(col.getOpType(), col.getOperand(0));
              newSelectlist.add(new AliasedColumn(col1, "agg"+aggColumnIdentiferNum));
              meta.getAggColumnAggAliasPairOfMaxMin().put(
                  new ImmutablePair<>(col.getOpType(), col1.getOperand(0)), "agg"+aggColumnIdentiferNum);
              maxminAlias.put("agg"+aggColumnIdentiferNum++, col.getOpType());
            }
          }
        }
        else {
          newSelectlist.add(selectItem);
        }
      } else {
        newSelectlist.add(selectItem);
      }
    }
    meta.setAggAlias(aggColumnAlias);
    meta.setMaxminAggAlias(maxminAlias);
    return newSelectlist;
  }

  void addTierColumn(SelectQuery query, List<SelectItem> newSelectList, ScrambleMeta scrambleMeta) {
    for (AbstractRelation table:query.getFromList()) {
      if (table instanceof BaseTable) {
        String schemaName = ((BaseTable) table).getSchemaName();
        String tableName = ((BaseTable) table).getTableName();
        if (scrambleMeta.isScrambled(schemaName, tableName) && scrambleMeta.getMetaForTable(schemaName, tableName).getNumberOfTiers()>1) {
          newSelectList.add(new AliasedColumn(new BaseColumn(schemaName, tableName, table.getAliasName().get(), scrambleMeta.getTierColumn(schemaName, tableName)),
              "verdictdbtier"+verdictdbTierIndentiferNum));
          query.addGroupby(new AliasReference("verdictdbtier"+verdictdbTierIndentiferNum++));
        }
      }
      else if (table instanceof JoinTable) {
        for (AbstractRelation jointable:((JoinTable) table).getJoinList()) {
          if (jointable instanceof BaseTable) {
            String schemaName = ((BaseTable) jointable).getSchemaName();
            String tableName = ((BaseTable) jointable).getTableName();
            if (scrambleMeta.isScrambled(schemaName, tableName) && scrambleMeta.getMetaForTable(schemaName, tableName).getNumberOfTiers()>1) {
              newSelectList.add(new AliasedColumn(new BaseColumn(schemaName, tableName, jointable.getAliasName().get(), scrambleMeta.getTierColumn(schemaName, tableName)),
                  "verdictdbtier"+verdictdbTierIndentiferNum));
              query.addGroupby(new AliasReference("verdictdbtier"+verdictdbTierIndentiferNum++));
            }
          }
        }
      }
    }
    verdictdbTierIndentiferNum = 0;
  }

}
