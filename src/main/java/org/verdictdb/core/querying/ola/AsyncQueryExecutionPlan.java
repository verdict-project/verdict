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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.core.execplan.ExecutableNode;
import org.verdictdb.core.querying.*;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.exception.VerdictDBValueException;

import java.util.*;

public class AsyncQueryExecutionPlan extends QueryExecutionPlan {

  private static final long serialVersionUID = -1670795390245860583L;

  private int aggColumnIdentiferNum = 0;

  private int verdictdbTierIndentiferNum = 0;

  static final String VERDICTDB_TIER_COLUMN_NAME = "verdictdb_tier_internal";

  private AsyncQueryExecutionPlan(String scratchpadSchemaName, ScrambleMetaSet scrambleMeta)
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
    ExecutableNodeBase newRoot = asyncPlan.makeAsyncronousAggIfAvailable(plan.getRootNode());
    asyncPlan.setRootNode(newRoot);
    return asyncPlan;
  }

  /**
   * Returns an asynchronous version of the given plan.
   *
   * @param root The root execution node of ALL nodes (i.e., not just the top agg node)
   * @return
   * @throws VerdictDBException
   */
  ExecutableNodeBase makeAsyncronousAggIfAvailable(ExecutableNodeBase root)
      throws VerdictDBException {
    List<AggExecutionNodeBlock> aggBlocks = identifyTopAggBlocks(scrambleMeta, root);

    // converted nodes should be used in place of the original nodes.
    for (int i = 0; i < aggBlocks.size(); i++) {
      // this node block contains the links to those nodes belonging to this block.
      AggExecutionNodeBlock nodeBlock = aggBlocks.get(i);
      SelectQuery originalQuery = null;
      if (nodeBlock.getBlockRootNode() instanceof AggExecutionNode) {
        originalQuery = ((AggExecutionNode) nodeBlock.getBlockRootNode()).getSelectQuery();
      }
      ExecutableNodeBase oldNode = nodeBlock.getBlockRootNode();
      //      ExecutableNodeBase newNode = nodeBlock.convertToProgressiveAgg(scrambleMeta);
      ExecutableNodeBase newNode = convertToProgressiveAgg(scrambleMeta, nodeBlock);
      if (newNode instanceof AsyncAggExecutionNode && originalQuery != null) {
        ((AsyncAggExecutionNode) newNode).setSelectQuery(originalQuery);
      }
      List<ExecutableNodeBase> parents = oldNode.getExecutableNodeBaseParents();
      for (ExecutableNodeBase parent : parents) {
        Integer channel = parent.getChannelForSource(oldNode);
        if (channel == null) {
          // do nothing
        } else {
          parent.cancelSubscriptionTo(oldNode);
          parent.subscribeTo(newNode, channel);
        }
      }
    }

    return root;
  }

  /**
   * Converts the root node and its descendants into the configuration that enables progressive
   * aggregation.
   *
   * <p>Basically aggregate subqueries are blocking operations while others operations are divided
   * into smaller- scale operations (which involve different portions of data).
   *
   * @param nodeBlock
   * @return Returns the root of the multiple aggregation nodes (each of which involves different
   *     combinations of partitions)
   * @throws VerdictDBValueException
   */
  public ExecutableNodeBase convertToProgressiveAgg(
      ScrambleMetaSet scrambleMeta, AggExecutionNodeBlock aggNodeBlock)
      throws VerdictDBValueException {

    List<ExecutableNodeBase> blockNodes = aggNodeBlock.getNodesInBlock();

    List<ExecutableNodeBase> individualAggNodes = new ArrayList<>();
    List<ExecutableNodeBase> combiners = new ArrayList<>();
    //    ScrambleMeta scrambleMeta = idCreator.getScrambleMeta();

    // First, plan how to perform block aggregation
    // filtering predicates that must inserted into different scrambled tables are identified.
    List<Pair<ExecutableNodeBase, Triple<String, String, String>>> scrambledNodes =
        identifyScrambledNodes(scrambleMeta, blockNodes);
    List<Pair<String, String>> scrambles = new ArrayList<>();
    for (Pair<ExecutableNodeBase, Triple<String, String, String>> a : scrambledNodes) {
      String schemaName = a.getRight().getLeft();
      String tableName = a.getRight().getMiddle();
      scrambles.add(Pair.of(schemaName, tableName));
    }
    OlaAggregationPlan aggPlan = new OlaAggregationPlan(scrambleMeta, scrambles);
    List<Pair<ExecutableNodeBase, ExecutableNodeBase>> oldSubscriptionInformation =
        new ArrayList<>();

    // Second, according to the plan, create individual nodes that perform aggregations.
    for (int i = 0; i < aggPlan.totalBlockAggCount(); i++) {

      // copy and remove the dependency to its parents
      oldSubscriptionInformation.clear();
      AggExecutionNodeBlock copy =
          aggNodeBlock.deepcopyExcludingDependentAggregates(oldSubscriptionInformation);
      AggExecutionNode aggroot = (AggExecutionNode) copy.getBlockRootNode();
      for (ExecutableNodeBase parent : aggroot.getExecutableNodeBaseParents()) {
        parent.cancelSubscriptionTo(aggroot); // not sure if this is required, but do anyway
      }
      aggroot.cancelSubscriptionsFromAllSubscribers(); // subscription will be reconstructed later.

      // Add extra predicates to restrain each aggregation to particular parts of base tables.
      List<Pair<ExecutableNodeBase, Triple<String, String, String>>> scrambledNodeAndTableName =
          identifyScrambledNodes(scrambleMeta, copy.getNodesInBlock());

      // Assign hyper table cube to the block
      aggroot.getAggMeta().setCubes(Arrays.asList(aggPlan.cubes.get(i)));

      // The new function performs both rewriting select list and adding tier columns
      // The important thing is that this job should be done starting from the leaf nodes
      // I created a skeleton function: rewriteSelectListOfRootAndListedDependents
      addTierColumnsRecursively(copy, aggroot, new HashSet<ExecutableNode>());

      // Insert predicates into individual aggregation nodes
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
          throw new VerdictDBValueException(
              String.format(
                  "The alias name for the table (%s, %s) is not found.", schemaName, tableName));
        }

        int left = span.getLeft();
        int right = span.getRight();
        if (left == right) {
          q.addFilterByAnd(
              ColumnOp.equal(
                  new BaseColumn(aliasName, aggblockColumn), ConstantColumn.valueOf(left)));
        } else {
          q.addFilterByAnd(
              ColumnOp.greaterequal(
                  new BaseColumn(aliasName, aggblockColumn), ConstantColumn.valueOf(left)));
          q.addFilterByAnd(
              ColumnOp.lessequal(
                  new BaseColumn(aliasName, aggblockColumn), ConstantColumn.valueOf(right)));
        }
      }

      individualAggNodes.add(aggroot);
    }

    // Third, stack combiners
    // clear existing broadcasting queues of individual agg nodes
    for (ExecutableNodeBase n : individualAggNodes) {
      n.cancelSubscriptionsFromAllSubscribers();
    }
    for (int i = 1; i < aggPlan.totalBlockAggCount(); i++) {
      AggCombinerExecutionNode combiner;
      if (i == 1) {
        combiner =
            AggCombinerExecutionNode.create(
                idCreator, individualAggNodes.get(0), individualAggNodes.get(1));
      } else {
        combiner =
            AggCombinerExecutionNode.create(
                idCreator, combiners.get(i - 2), individualAggNodes.get(i));
      }
      combiners.add(combiner);
    }

    // Fourth, re-link the subscription relationship for the new AsyncAggNode
    ExecutableNodeBase newRoot =
        AsyncAggExecutionNode.create(idCreator, individualAggNodes, combiners, scrambleMeta);
    // Set hashmap of tier column alias for AsyncAggNode
    setTierColumnAlias((AsyncAggExecutionNode) newRoot);

    // Finally remove the old subscription information: old copied node -> still used old node
    for (Pair<ExecutableNodeBase, ExecutableNodeBase> parentToSource : oldSubscriptionInformation) {
      ExecutableNodeBase subscriber = parentToSource.getLeft();
      ExecutableNodeBase source = parentToSource.getRight();
      subscriber.cancelSubscriptionTo(source);
    }

    return newRoot;
  }

  /**
   * @param scrambleMeta Information about what tables have been scrambled.
   * @param blockNodes
   * @return ExecutableNodeBase is the reference to the scrambled base table. The triple is (schema,
   *     table, alias) of scrambled tables.
   */
  private static List<Pair<ExecutableNodeBase, Triple<String, String, String>>>
      identifyScrambledNodes(ScrambleMetaSet scrambleMeta, List<ExecutableNodeBase> blockNodes) {

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

  private List<ColumnOp> getAggregateColumn(UnnamedColumn sel) {
    List<SelectItem> itemToCheck = new ArrayList<>();
    itemToCheck.add(sel);
    List<ColumnOp> columnOps = new ArrayList<>();
    while (!itemToCheck.isEmpty()) {
      SelectItem s = itemToCheck.get(0);
      itemToCheck.remove(0);
      if (s instanceof ColumnOp) {
        if (((ColumnOp) s).getOpType().equals("count")
            || ((ColumnOp) s).getOpType().equals("sum")
            || ((ColumnOp) s).getOpType().equals("avg")
            || ((ColumnOp) s).getOpType().equals("max")
            || ((ColumnOp) s).getOpType().equals("min")) {
          columnOps.add((ColumnOp) s);
        } else itemToCheck.addAll(((ColumnOp) s).getOperands());
      }
    }
    return columnOps;
  }

  /**
   * identify the nodes that are (1) aggregates with scrambled tables, (2) no descendants of any
   * other top aggregates, (3) the aggregated columns (inner-most base columns if they are inside
   * some functions) must include only the columns from the scrambled tables.
   */
  private List<AggExecutionNodeBlock> identifyTopAggBlocks(
      ScrambleMetaSet scrambleMeta, ExecutableNodeBase root) {
    List<AggExecutionNodeBlock> aggblocks = new ArrayList<>();
    //    ScrambleMeta scrambleMeta = root.getPlan().getScrambleMeta();

    if (root instanceof AggExecutionNode) {
      // check if it contains at least one scrambled table.
      if (doesContainScramble(root, scrambleMeta)) {
        AggExecutionNodeBlock block = new AggExecutionNodeBlock(root);
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

  private boolean doesContainScramble(ExecutableNodeBase node, ScrambleMetaSet scrambleMeta) {
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

  private List<BaseTable> getMultiTierScramble(
      ExecutableNodeBase node, ScrambleMetaSet scrambleMeta) {
    SelectQuery query = ((QueryNodeBase) node).getSelectQuery();
    List<BaseTable> multiTierScrambleTables = new ArrayList<>();
    // check within the query
    for (AbstractRelation rel : query.getFromList()) {
      if (rel instanceof BaseTable) {
        BaseTable base = (BaseTable) rel;
        String schemaName = base.getSchemaName();
        String tableName = base.getTableName();
        if (scrambleMeta.isScrambled(schemaName, tableName)
            && scrambleMeta.getMetaForTable(schemaName, tableName).getNumberOfTiers() > 1) {
          multiTierScrambleTables.add(base);
        }
      } else if (rel instanceof JoinTable) {
        for (AbstractRelation r : ((JoinTable) rel).getJoinList()) {
          if (r instanceof BaseTable) {
            BaseTable base = (BaseTable) r;
            String schemaName = base.getSchemaName();
            String tableName = base.getTableName();
            if (scrambleMeta.isScrambled(schemaName, tableName)
                && scrambleMeta.getMetaForTable(schemaName, tableName).getNumberOfTiers() > 1) {
              multiTierScrambleTables.add(base);
            }
          }
        }
      }
      // SelectQuery is not supposed to be passed.
    }

    return multiTierScrambleTables;
  }

  /**
   * Rewrite the select list of the nodes in a block; the nodes that belong ot the block are
   * identified by the second parameter `nodeList`.
   *
   * @param root
   * @param nodeList
   * @return
   */
  private ExecutableNodeBase rewriteSelectListOfRootAndListedDependents(
      ExecutableNodeBase root, List<ExecutableNodeBase> nodeList) {
    List<ExecutableNodeBase> visitList = new ArrayList<>();
    ExecutableNodeBase rewritten =
        rewriteSelectListOfRootAndListedDependentsInner(root, nodeList, visitList);
    return rewritten;
  }

  private ExecutableNodeBase rewriteSelectListOfRootAndListedDependentsInner(
      ExecutableNodeBase root,
      List<ExecutableNodeBase> nodeList,
      List<ExecutableNodeBase> visitList) {

    // base condition: return immediately if the root does not belong to nodeList.

    // base condition: return immediately if this node has been visited before.

    // call rewriteSelectListOfRootAndListedDependents for all dependents first

    // then does the job for the root node.

    return null;
  }

  private List<SelectItem> rewriteSelectlistWithBasicAgg(SelectQuery query, AggMeta meta) {

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
          for (ColumnOp col : columnOps) {
            if (col.getOpType().equals("avg")) {
              if (!meta.getAggColumnAggAliasPair()
                  .containsKey(new ImmutablePair<>("sum", col.getOperand(0)))) {
                ColumnOp col1 = new ColumnOp("sum", col.getOperand(0));
                newSelectlist.add(new AliasedColumn(col1, "agg" + aggColumnIdentiferNum));
                meta.getAggColumnAggAliasPair()
                    .put(
                        new ImmutablePair<>("sum", col1.getOperand(0)),
                        "agg" + aggColumnIdentiferNum);
                aggColumnAlias.add("agg" + aggColumnIdentiferNum++);
              }
              if (!meta.getAggColumnAggAliasPair()
                  .containsKey(
                      new ImmutablePair<>("count", (UnnamedColumn) new AsteriskColumn()))) {
                ColumnOp col2 = new ColumnOp("count", new AsteriskColumn());
                newSelectlist.add(new AliasedColumn(col2, "agg" + aggColumnIdentiferNum));
                meta.getAggColumnAggAliasPair()
                    .put(
                        new ImmutablePair<>("count", (UnnamedColumn) new AsteriskColumn()),
                        "agg" + aggColumnIdentiferNum);
                aggColumnAlias.add("agg" + aggColumnIdentiferNum++);
              }
            } else if (col.getOpType().equals("count") || col.getOpType().equals("sum")) {
              if (col.getOpType().equals("count")
                  && !meta.getAggColumnAggAliasPair()
                      .containsKey(
                          new ImmutablePair<>("count", (UnnamedColumn) (new AsteriskColumn())))) {
                ColumnOp col1 = new ColumnOp(col.getOpType());
                newSelectlist.add(new AliasedColumn(col1, "agg" + aggColumnIdentiferNum));
                meta.getAggColumnAggAliasPair()
                    .put(
                        new ImmutablePair<>(col.getOpType(), (UnnamedColumn) new AsteriskColumn()),
                        "agg" + aggColumnIdentiferNum);
                aggColumnAlias.add("agg" + aggColumnIdentiferNum++);
              } else if (col.getOpType().equals("sum")
                  && !meta.getAggColumnAggAliasPair()
                      .containsKey(new ImmutablePair<>(col.getOpType(), col.getOperand(0)))) {
                ColumnOp col1 = new ColumnOp(col.getOpType(), col.getOperand(0));
                newSelectlist.add(new AliasedColumn(col1, "agg" + aggColumnIdentiferNum));
                meta.getAggColumnAggAliasPair()
                    .put(
                        new ImmutablePair<>(col.getOpType(), col1.getOperand(0)),
                        "agg" + aggColumnIdentiferNum);
                aggColumnAlias.add("agg" + aggColumnIdentiferNum++);
              }
            } else if (col.getOpType().equals("max") || col.getOpType().equals("min")) {
              ColumnOp col1 = new ColumnOp(col.getOpType(), col.getOperand(0));
              newSelectlist.add(new AliasedColumn(col1, "agg" + aggColumnIdentiferNum));
              meta.getAggColumnAggAliasPairOfMaxMin()
                  .put(
                      new ImmutablePair<>(col.getOpType(), col1.getOperand(0)),
                      "agg" + aggColumnIdentiferNum);
              maxminAlias.put("agg" + aggColumnIdentiferNum++, col.getOpType());
            }
          }
        } else {
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

  /**
   * Adds tier expressions to the individual aggregates
   *
   * @param query
   * @param newSelectList
   * @param scrambleMeta
   */
  private void addTierColumn(
      SelectQuery query,
      List<SelectItem> newSelectList,
      ScrambleMetaSet scrambleMeta,
      List<ProjectionNode> projectionNodeSources) {
    for (AbstractRelation table : query.getFromList()) {
      if (table instanceof BaseTable) {
        String schemaName = ((BaseTable) table).getSchemaName();
        String tableName = ((BaseTable) table).getTableName();
        if (scrambleMeta.isScrambled(schemaName, tableName)
            && scrambleMeta.getMetaForTable(schemaName, tableName).getNumberOfTiers() > 1) {
          newSelectList.add(
              new AliasedColumn(
                  new BaseColumn(
                      schemaName,
                      tableName,
                      table.getAliasName().get(),
                      scrambleMeta.getTierColumn(schemaName, tableName)),
                  VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum));
          query.addGroupby(
              new AliasReference(VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum++));
        }
      } else if (table instanceof JoinTable) {
        for (AbstractRelation jointable : ((JoinTable) table).getJoinList()) {
          if (jointable instanceof BaseTable) {
            String schemaName = ((BaseTable) jointable).getSchemaName();
            String tableName = ((BaseTable) jointable).getTableName();
            if (scrambleMeta.isScrambled(schemaName, tableName)
                && scrambleMeta.getMetaForTable(schemaName, tableName).getNumberOfTiers() > 1) {
              newSelectList.add(
                  new AliasedColumn(
                      new BaseColumn(
                          schemaName,
                          tableName,
                          jointable.getAliasName().get(),
                          scrambleMeta.getTierColumn(schemaName, tableName)),
                      VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum));
              query.addGroupby(
                  new AliasReference(VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum++));
            }
          }
        }
      }
    }

    // Add possible tier column if its sources are projectionNode
    for (ProjectionNode source : projectionNodeSources) {
      for (Map.Entry<ScrambleMeta, String> entry :
          source.getAggMeta().getScrambleTableTierColumnAlias().entrySet()) {
        String oldtierAlias = entry.getValue();
        // Add tier column to select list
        SelectItem selectItem;
        String tierColumnAlias = VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum++;
        if (source.getSelectQuery().getAliasName().isPresent()) {
          String sourceAlias = source.getSelectQuery().getAliasName().get();
          selectItem =
              new AliasedColumn(new BaseColumn(sourceAlias, oldtierAlias), tierColumnAlias);
        } else {
          selectItem = new AliasedColumn(new BaseColumn(oldtierAlias), tierColumnAlias);
        }
        newSelectList.add(selectItem);
        query.addGroupby(new AliasReference(tierColumnAlias));
      }
    }
    verdictdbTierIndentiferNum = 0;
  }

  private void rewriteProjectionNodeForMultiTier(
      ProjectionNode node, List<BaseTable> MultiTiertables, ScrambleMetaSet scrambleMeta) {
    List<SelectItem> selectItemList = node.getSelectQuery().getSelectList();
    if (selectItemList.get(0) instanceof AsteriskColumn) {
      for (BaseTable t : MultiTiertables) {
        String tierColumnAlias = VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum++;
        // Record the tier column alias with its corresponding scramble table
        ScrambleMeta meta = scrambleMeta.getMetaForTable(t.getSchemaName(), t.getTableName());
        node.getAggMeta().getScrambleTableTierColumnAlias().put(meta, tierColumnAlias);
      }
    } else {
      for (BaseTable t : MultiTiertables) {
        // Add tier column to the select list
        String tierColumnName = scrambleMeta.getTierColumn(t.getSchemaName(), t.getTableName());
        SelectItem tierColumn;
        String tierColumnAlias = VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum++;
        if (t.getAliasName().isPresent()) {
          tierColumn =
              new AliasedColumn(
                  new BaseColumn(t.getAliasName().get(), tierColumnName), tierColumnAlias);
        } else {
          tierColumn =
              new AliasedColumn(new BaseColumn(t.getTableName(), tierColumnName), tierColumnAlias);
        }
        selectItemList.add(tierColumn);

        // Record the tier column alias with its corresponding scramble table
        ScrambleMeta meta = scrambleMeta.getMetaForTable(t.getSchemaName(), t.getTableName());
        node.getAggMeta().getScrambleTableTierColumnAlias().put(meta, tierColumnAlias);
      }
    }
    verdictdbTierIndentiferNum = 0;
  }

  public void rewrittenProjectionNode(AggExecutionNodeBlock block, ProjectionNode node) {
    // If it is a leaf node, check whether it contains scramble table
    if (node.getSources().size() == 0) {
      List<BaseTable> multiTierScrambleTables = getMultiTierScramble(node, scrambleMeta);
      // rewrite itself
      if (!multiTierScrambleTables.isEmpty()) {
        rewriteProjectionNodeForMultiTier(node, multiTierScrambleTables, scrambleMeta);
      }
      return;
    }

    List<ProjectionNode> projectionNodesSources = new ArrayList<>();
    for (ExecutableNode source : node.getSources()) {
      if (source instanceof ProjectionNode && block.getNodesInBlock().contains(source)) {
        projectionNodesSources.add((ProjectionNode) source);
      }
    }

    // Add tier column if its placeholder table has scramble table
    List<SelectItem> selectItemList = node.getSelectQuery().getSelectList();
    boolean isAsterisk = selectItemList.get(0) instanceof AsteriskColumn;
    for (ProjectionNode source : projectionNodesSources) {
      for (Map.Entry<ScrambleMeta, String> entry :
          source.getAggMeta().getScrambleTableTierColumnAlias().entrySet()) {
        String oldtierAlias = entry.getValue();
        String tierColumnAlias = VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum++;
        if (!isAsterisk) {
          // Add tier column to select list
          SelectItem selectItem;
          if (source.getSelectQuery().getAliasName().isPresent()) {
            String sourceAlias = source.getSelectQuery().getAliasName().get();
            selectItem =
                new AliasedColumn(new BaseColumn(sourceAlias, oldtierAlias), tierColumnAlias);
          } else {
            selectItem = new AliasedColumn(new BaseColumn(oldtierAlias), tierColumnAlias);
          }
          selectItemList.add(selectItem);
        }
        // Construct tier column Map
        node.getAggMeta().getScrambleTableTierColumnAlias().put(entry.getKey(), tierColumnAlias);
      }
    }
    List<BaseTable> multiTierScrambleTables = getMultiTierScramble(node, scrambleMeta);

    // Add tier column if itself contain scramble table
    if (!multiTierScrambleTables.isEmpty()) {
      rewriteProjectionNodeForMultiTier(node, multiTierScrambleTables, scrambleMeta);
    }
    verdictdbTierIndentiferNum = 0;
  }

  public void setTierColumnAlias(AsyncAggExecutionNode node) {
    AggExecutionNode aggNode = (AggExecutionNode) node.getSources().get(0);
    List<ProjectionNode> projectionNodeList = new ArrayList<>();
    for (ExecutableNodeBase source : aggNode.getSources()) {
      if (source instanceof ProjectionNode) {
        projectionNodeList.add((ProjectionNode) source);
      }
    }
    // Rewrite itself to add tier column
    for (ProjectionNode source : projectionNodeList) {
      for (Map.Entry<ScrambleMeta, String> entry :
          source.getAggMeta().getScrambleTableTierColumnAlias().entrySet()) {
        // Construct tier column Map
        String tierColumnAlias = VERDICTDB_TIER_COLUMN_NAME + verdictdbTierIndentiferNum++;
        node.getAggMeta().getScrambleTableTierColumnAlias().put(entry.getKey(), tierColumnAlias);
      }
    }

    verdictdbTierIndentiferNum = 0;
  }

  void addTierColumnsRecursively(
      AggExecutionNodeBlock block, ExecutableNodeBase node, Set<ExecutableNode> visitList) {

    // rewrite all the sources
    for (ExecutableNodeBase source : node.getSources()) {
      if (!visitList.contains(source) && block.getNodesInBlock().contains(source)) {
        addTierColumnsRecursively(block, source, visitList);
      }
    }

    // rewrite the current node
    visitList.add(node);
    if (node instanceof AggExecutionNode) {
      List<SelectItem> newSelectlist =
          rewriteSelectlistWithBasicAgg(
              ((AggExecutionNode) node).getSelectQuery(), node.getAggMeta());
      List<ProjectionNode> projectionNodeList = new ArrayList<>();
      for (ExecutableNodeBase source : node.getSources()) {
        if (source instanceof ProjectionNode && block.getNodesInBlock().contains(source)) {
          projectionNodeList.add((ProjectionNode) source);
        }
      }
      addTierColumn(
          ((AggExecutionNode) node).getSelectQuery(),
          newSelectlist,
          scrambleMeta,
          projectionNodeList);
      ((AggExecutionNode) node).getSelectQuery().clearSelectList();
      ((AggExecutionNode) node).getSelectQuery().getSelectList().addAll(newSelectlist);
      aggColumnIdentiferNum = 0;
    } else if (node instanceof ProjectionNode) {
      rewrittenProjectionNode(block, (ProjectionNode) node);
    }
  }
}
