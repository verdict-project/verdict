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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.*;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;
import scala.reflect.internal.Constants.Constant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static java.util.Map.Entry;

/**
 * Represents an "progressive" execution of a single aggregate query (without nested components).
 *
 * <p>Steps: 1. identify agg and nonagg columns of a given select agg query. 2. convert the query
 * into multiple block-agg queries. 3. issue those block-agg queries one by one. 4. combine the
 * results of those block-agg queries as the answers to those queries arrive. 5. depending on the
 * interface, call an appropriate result handler.
 *
 * @author Yongjoo Park
 */
public class AsyncAggExecutionNode extends ProjectionNode {

  private static final long serialVersionUID = -1829554239432075523L;

  private static final String INNER_RAW_AGG_TABLE_ALIAS = "verdictdb_internal_before_scaling";

  private static final String TIER_CONSOLIDATED_TABLE_ALIAS = "verdictdb_internal_tier_consolidated";

  //  private static final String SCRAMBLE_META_STORE_KEY = "scrambleMeta";

  private ScrambleMetaSet scrambleMeta;

//  private String newTableSchemaName;
//
//  private String newTableName;

  // This is the list of aggregate columns contained in the selectQuery field.
  private List<ColumnOp> aggColumns;

  private Map<Integer, String> scrambledTableTierInfo;

  int tableNum = 1;

  private AsyncAggExecutionNode(IdCreator idCreator) {
    super(idCreator, null);
  }

  /**
   * A factory method for AsyncAggExecutionNode.
   *
   * This static method performs the following operations:
   * 1. Link individual aggregate nodes and combiners appropriately.
   * 2. Create a base table which includes several placeholders
   *     - scale factor placeholder
   *     - temp table placeholder
   *
   * @param idCreator
   * @param individualAggs
   * @param combiners
   * @param meta
   * @return
   */
  public static AsyncAggExecutionNode create(
      IdCreator idCreator,
      List<ExecutableNodeBase> individualAggs,
      List<ExecutableNodeBase> combiners,
      ScrambleMetaSet meta) {

    AsyncAggExecutionNode node = new AsyncAggExecutionNode(idCreator);

    // this placeholder base table is used for query construction later
    String alias = INNER_RAW_AGG_TABLE_ALIAS;
    Pair<BaseTable, SubscriptionTicket> tableAndTicket = node.createPlaceHolderTable(alias);
    BaseTable placeholderTable = tableAndTicket.getLeft();
    SubscriptionTicket ticket = tableAndTicket.getRight();

    // first agg -> root
    AggExecutionNode firstSource = (AggExecutionNode) individualAggs.get(0);
    firstSource.registerSubscriber(ticket);
    //    node.subscribeTo(individualAggs.get(0), 0);

    // combiners -> root
    for (ExecutableNodeBase c : combiners) {
      c.registerSubscriber(ticket);
      //      node.subscribeTo(c, 0);
    }

    node.setScrambleMetaSet(meta);   // the scramble meta must be not be shared; thus, thread-safe
    node.setNamer(idCreator);        // the name can be shared

    // creates a base query that contain placeholders
    AggMeta sourceAggMeta = firstSource.getAggMeta();
    List<SelectItem> sourceSelectList = firstSource.getSelectQuery().getSelectList();
    Triple<List<ColumnOp>, SqlConvertible, Map<Integer, String>> aggColumnsAndQuery =
        createBaseQueryForReplacement(sourceAggMeta, sourceSelectList, placeholderTable, meta);
    node.aggColumns = aggColumnsAndQuery.getLeft();
    SelectQuery subquery = (SelectQuery) aggColumnsAndQuery.getMiddle();
    node.selectQuery = sumUpTierGroup(subquery, sourceAggMeta);
    node.scrambledTableTierInfo = new ImmutableMap.Builder<Integer, String>()
        .putAll(aggColumnsAndQuery.getRight())
        .build();
    return node;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    super.createQuery(tokens);

    ExecutionInfoToken token = tokens.get(0);
    AggMeta sourceAggMeta = (AggMeta) token.getValue("aggMeta");

    // First, calculate the scale factor and use it to replace the scale factor placeholder
    //    HashMap<List<Integer>, Double> scaleFactor =
    //        calculateScaleFactor(sourceAggMeta, scrambledTableTierInfo);

    Map<UnnamedColumn, Double> conditionToScaleFactor =
        computeScaleFactorForTierCombinations(sourceAggMeta, INNER_RAW_AGG_TABLE_ALIAS);

//    // we store the original aggColumns to restore it later.
//    List<ColumnOp> clonedAggColumns = new ArrayList<>();
//    for (ColumnOp col : aggColumns) {
//      clonedAggColumns.add(col.deepcopy());
//    }

    // update the agg column scaling factor
    List<UnnamedColumn> scalingOperands = new ArrayList<>();
    for (Entry<UnnamedColumn, Double> condToScale : conditionToScaleFactor.entrySet()) {
      UnnamedColumn cond = condToScale.getKey();
      double scale = condToScale.getValue();
      scalingOperands.add(cond);
      scalingOperands.add(ConstantColumn.valueOf(scale));
    }
    scalingOperands.add(ConstantColumn.valueOf(1.0));     // default scaling factor is always 1.0
    ColumnOp scalingColumn = ColumnOp.casewhen(scalingOperands);
    for (ColumnOp aggcol : aggColumns) {
      aggcol.setOperand(0, scalingColumn);
    }

    //    // single-tier case
    //    if (scaleFactor.size() == 1) {
    //      // Substitute the scale factor
    //      Double s = (Double) (scaleFactor.values().toArray())[0];
    //      for (ColumnOp col : aggColumns) {
    //        col.setOperand(0, ConstantColumn.valueOf(String.format("%.16f", s)));
    //      }
    //    }
    //    // multiple-tiers case
    //    else {
    //      // If it has multiple tiers, we need to rewrite the multiply column into when-then-else column
    //      for (ColumnOp col : aggColumns) {
    //        //        String alias = ((BaseColumn) col.getOperand(1)).getColumnName();
    //        col.setOpType("casewhen");
    //        List<UnnamedColumn> operands = new ArrayList<>();
    //        for (Map.Entry<List<Integer>, Double> entry : scaleFactor.entrySet()) {
    //          List<Integer> tierPermutation = entry.getKey();
    //          UnnamedColumn condition = generateCaseCondition(tierPermutation, scrambledTableTierInfo);
    //          operands.add(condition);
    //          ColumnOp multiply =
    //              new ColumnOp(
    //                  "multiply",
    //                  Arrays.asList(
    //                      ConstantColumn.valueOf(String.format("%.16f", entry.getValue())),
    //                      col.getOperand(1)));
    //          operands.add(multiply);
    //        }
    //        operands.add(ConstantColumn.valueOf(0));
    //        col.setOperand(operands);
    //      }
    //    }

    // If it has multiple tiers, we need to sum up the result
    // We treat both single-tier and multi-tier cases simultaneously here to make alias management
    // easier.
    //      SelectQuery query = sumUpTierGroup(selectQuery.deepcopy(), sourceAggMeta);

//    // Restore the original aggColumns
//    for (int i = 0; i < clonedAggColumns.size(); i++) {
//      ColumnOp cloned = clonedAggColumns.get(i);
//      ColumnOp originalReference = aggColumns.get(i);
//      originalReference.setOpType(cloned.getOpType());
//      originalReference.setOperand(cloned.getOperands());
//    }

    //    if (multipleTierTableTierInfo.size() > 0) {
    //      query = sumUpTierGroup(
    //          (SelectQuery) aggColumnsAndQuery.getRight(), ((AggMeta) token.getValue("aggMeta")));
    //    } else {
    //      query = (SelectQuery) aggColumnsAndQuery.getRight();
    //    }

    //    Pair<String, String> tempTableFullName = getNamer().generateTempTableName();
    //    newTableSchemaName = tempTableFullName.getLeft();
    //    newTableName = tempTableFullName.getRight();
    selectQuery = replaceWithOriginalSelectList(selectQuery, sourceAggMeta);

    //    // TODO: this logic should have been placed in the root
    //    if (selectQuery != null) {
    //      if (!selectQuery.getGroupby().isEmpty() && selectQuery.getHaving().isPresent()) {
    //        createTableQuery.addGroupby(selectQuery.getGroupby());
    //      }
    //      if (!selectQuery.getOrderby().isEmpty()) {
    //        createTableQuery.addOrderby(selectQuery.getOrderby());
    //      }
    //      if (selectQuery.getHaving().isPresent()) {
    //        createTableQuery.addHavingByAnd(selectQuery.getHaving().get());
    //      }
    //      if (selectQuery.getLimit().isPresent()) {
    //        createTableQuery.addLimit(selectQuery.getLimit().get());
    //      }
    //    }

    //    CreateTableAsSelectQuery createQuery =
    //        new CreateTableAsSelectQuery(newTableSchemaName, newTableName, createTableQuery);
    return super.createQuery(tokens);
  }

  /**
   *
   * @param sourceAggMeta The aggmeta of the downstream node
   * @return Map of a filtering condition to the scaling factor
   */
  private Map<UnnamedColumn, Double> computeScaleFactorForTierCombinations(
      AggMeta sourceAggMeta, String sourceTableAlias) {
    Map<UnnamedColumn, Double> scalingFactorPerTier = new HashMap<>();

    Map<TierCombination, Double> scaleFactors = sourceAggMeta.computeScaleFactors();
    Map<ScrambleMeta, String> tierColums = sourceAggMeta.getTierColumnForScramble();

    // each iteration of this loop generates a single condition-then part
    for (Entry<TierCombination, Double> tierScale : scaleFactors.entrySet()) {
      UnnamedColumn tierCombinationCondition = null;
      TierCombination combination = tierScale.getKey();
      double scale = tierScale.getValue();

      // each iteration of this loop generates a condition that must be AND-connected
      // to compose the condition for an entire tier combination.
      for (Entry<Pair<String, String>, Integer> perTable : combination) {
        Pair<String, String> table = perTable.getKey();
        Integer tier = perTable.getValue();
        String aliasName = findScrambleAlias(tierColums, table);

        UnnamedColumn part = ColumnOp.equal(
            new BaseColumn(sourceTableAlias, aliasName),
            ConstantColumn.valueOf(tier));

        if (tierCombinationCondition == null) {
          tierCombinationCondition = part;
        } else {
          tierCombinationCondition = ColumnOp.and(tierCombinationCondition, part);
        }
      }

      scalingFactorPerTier.put(tierCombinationCondition, scale);
    }

    return scalingFactorPerTier;
  }

  private String findScrambleAlias(
      Map<ScrambleMeta, String> tierColums, Pair<String, String> table) {

    for (Entry<ScrambleMeta, String> metaToAlias : tierColums.entrySet()) {
      ScrambleMeta meta = metaToAlias.getKey();
      String aliasName = metaToAlias.getValue();
      if (meta.getSchemaName().equals(table.getLeft()) &&
          meta.getTableName().equals(table.getRight())) {
        return aliasName;
      }
    }
    return null;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = super.createToken(result);
    return token;
  }

  public ScrambleMetaSet getScrambleMeta() {
    //    return (ScrambleMetaSet) retrieveStoredObjectThreadSafely(SCRAMBLE_META_STORE_KEY);
    return scrambleMeta;
  }

  public void setScrambleMetaSet(ScrambleMetaSet meta) {
    //    storeObjectThreadSafely(SCRAMBLE_META_STORE_KEY, meta);
    this.scrambleMeta = meta;
  }

  /**
   * @param sourceAggMeta AggMeta instance passed from a downstream node (either an individual
   *                      aggregate node or a combiner node). This object contains what are the
   *                      tier columns for the scrambled tables they cover and what aggregates
   *                      are being computed.
   * @return Key: aggregate column list
   */
  private static Triple<List<ColumnOp>, SqlConvertible, Map<Integer, String>>
  createBaseQueryForReplacement(
      AggMeta sourceAggMeta,
      List<SelectItem> sourceSelectList,
      BaseTable placeholderTable,
      ScrambleMetaSet metaSet) {

    Map<Integer, String> multipleTierTableTierInfo = new HashMap<>();
    List<ColumnOp> aggColumnlist = new ArrayList<>();
    //    ScrambleMetaSet scrambleMetaSet = getScrambleMeta();

    List<HyperTableCube> cubes = sourceAggMeta.getCubes();
    //    SelectQuery dependentQuery = (SelectQuery) token.getValue("dependentQuery");

    //    dependentQuery.deepcopy().getSelectList();
    List<SelectItem> newSelectList = new ArrayList<>(sourceSelectList);
    //    AggMeta aggMeta = (AggMeta) token.getValue("aggMeta");

    for (SelectItem selectItem : newSelectList) {
      if (selectItem instanceof AliasedColumn) {
        AliasedColumn aliasedColumn = (AliasedColumn) selectItem;
        int index = newSelectList.indexOf(selectItem);
        UnnamedColumn col = aliasedColumn.getColumn();

        if (sourceAggMeta.getAggAlias().contains(aliasedColumn.getAliasName())) {
          ColumnOp aggColumn =
              ColumnOp.multiply(
                  ConstantColumn.valueOf(1.0),
                  new BaseColumn(INNER_RAW_AGG_TABLE_ALIAS, aliasedColumn.getAliasName()));
          aggColumnlist.add(aggColumn);
          newSelectList.set(index, new AliasedColumn(aggColumn, aliasedColumn.getAliasName()));
        } else if (sourceAggMeta.getMaxminAggAlias().keySet().contains(aliasedColumn.getAliasName())) {
          newSelectList.set(
              index,
              new AliasedColumn(
                  new BaseColumn(INNER_RAW_AGG_TABLE_ALIAS, aliasedColumn.getAliasName()),
                  aliasedColumn.getAliasName()));
        } else {
          // Looking for tier column
          //          if (!Initiated && col instanceof BaseColumn) {
          if (col instanceof BaseColumn) {
            String schemaName = ((BaseColumn) col).getSchemaName();
            String tableName = ((BaseColumn) col).getTableName();
            if (metaSet.isScrambled(schemaName, tableName)
                && ((BaseColumn) col)
                .getColumnName()
                .equals(metaSet.getTierColumn(schemaName, tableName))) {
              for (Dimension d : cubes.get(0).getDimensions()) {
                if (d.getTableName().equals(tableName) && d.getSchemaName().equals(schemaName)) {
                  multipleTierTableTierInfo.put(
                      cubes.get(0).getDimensions().indexOf(d), aliasedColumn.getAliasName());
                  break;
                }
              }
            }
          }

          newSelectList.set(
              index,
              new AliasedColumn(
                  new BaseColumn(INNER_RAW_AGG_TABLE_ALIAS, aliasedColumn.getAliasName()),
                  aliasedColumn.getAliasName()));
        }
      }
    }

    // Setup from table
    SelectQuery query = SelectQuery.create(newSelectList,placeholderTable);

    return Triple.of(aggColumnlist, (SqlConvertible) query, multipleTierTableTierInfo);
  }

  /**
   * Currently, assume block size is uniform
   *
   * @return Return tier permutation list and its scale factor
   */
  private HashMap<List<Integer>, Double> calculateScaleFactor(
      AggMeta sourceAggMeta,
      Map<Integer, String> multipleTierTableTierInfo)
          throws VerdictDBValueException {

    List<HyperTableCube> cubes = sourceAggMeta.getCubes();
    ScrambleMetaSet scrambleMetaSet = this.getScrambleMeta();
    List<ScrambleMeta> metaForTablesList = new ArrayList<>();
    List<Integer> blockCountList = new ArrayList<>();
    List<Pair<Integer, Integer>> scrambleTableTierInfo = new ArrayList<>();   // block span index for each table

    for (Dimension d : cubes.get(0).getDimensions()) {
      ScrambleMeta scrambleMeta = scrambleMetaSet.getSingleMeta(d.getSchemaName(), d.getTableName());

      blockCountList.add(
          scrambleMetaSet.getAggregationBlockCount(d.getSchemaName(), d.getTableName()));
      metaForTablesList.add(scrambleMeta);
      scrambleTableTierInfo.add(
          new ImmutablePair<>(
              cubes.get(0).getDimensions().indexOf(d),
              scrambleMetaSet
              .getSingleMeta(d.getSchemaName(), d.getTableName())
              .getNumberOfTiers()));

      //      if (scrambleMeta.getNumberOfTiers() > 1 && !Initiated) {
      if (scrambleMeta.getNumberOfTiers() > 1) {
        //        ScrambleMeta meta = scrambleMetaSet.getSingleMeta(d.getSchemaName(), d.getTableName());
        Map<ScrambleMeta, String> scrambleTableTierColumnAlias =
            sourceAggMeta.getTierColumnForScramble();

        if (scrambleTableTierColumnAlias.containsKey(scrambleMeta) == false) {
          throw new VerdictDBValueException("The metadata for a scrambled table is not found.");
        }

        // TODO: move this somewhere
        //        multipleTierTableTierInfo.put(
        //            cubes.get(0).getDimensions().indexOf(d),
        //            tierColumnForScramble.get(scrambleMeta));

        //        if (tierColumnForScramble.containsKey(meta)) {
        //
        //        }
        //        else {
        //          multipleTierTableTierInfo.put(
        //              cubes.get(0).getDimensions().indexOf(d),
        //              scrambleMeta.getSingleMeta(d.getSchemaName(), d.getTableName()).getTierColumn());
        //        }
      }
    }

    List<List<Integer>> tierPermuation = generateTierPermuation(scrambleTableTierInfo);
    HashMap<List<Integer>, Double> scaleFactor = new HashMap<>();
    for (List<Integer> tierlist : tierPermuation) {
      double total = 0;
      for (HyperTableCube cube : cubes) {
        double scale = 1;
        for (int i = 0; i < tierlist.size(); i++) {
          int tier = tierlist.get(i);
          Dimension d = cube.getDimensions().get(i);

          double prob;
          if (d.getBegin() == 0) {
            prob = metaForTablesList.get(i).getCumulativeDistributionForTier(tier).get(d.getEnd());
          } else {
            prob = metaForTablesList.get(i).getCumulativeDistributionForTier(tier).get(d.getEnd())
                - metaForTablesList
                .get(i)
                .getCumulativeDistributionForTier(tier)
                .get(d.getBegin() - 1);
          }

          scale = scale * prob;
        }
        total += scale;
      }

      if (total == 0) {
        scaleFactor.put(tierlist, 0.0);
      } else {
        scaleFactor.put(tierlist, 1.0 / total);
      }
    }
    return scaleFactor;
  }

  /**
   * Generate the permuation of multiple tiers
   *
   * @param scrambleTableTierInfo
   * @return
   */
  private List<List<Integer>> generateTierPermuation(List<Pair<Integer, Integer>> scrambleTableTierInfo) {
    if (scrambleTableTierInfo.size() == 1) {
      List<List<Integer>> res = new ArrayList<>();
      for (int tier = 0; tier < scrambleTableTierInfo.get(0).getRight(); tier++) {
        res.add(Arrays.asList(tier));
      }
      return res;
    } else {
      List<Pair<Integer, Integer>> next =
          scrambleTableTierInfo.subList(1, scrambleTableTierInfo.size());
      List<List<Integer>> subres = generateTierPermuation(next);
      List<List<Integer>> res = new ArrayList<>();
      for (int tier = 0; tier < scrambleTableTierInfo.get(0).getRight(); tier++) {
        for (List<Integer> tierlist : subres) {
          List<Integer> newTierlist = new ArrayList<>();
          for (int i : tierlist) {
            newTierlist.add(Integer.valueOf(i));
          }
          newTierlist.add(0, tier);
          res.add(newTierlist);
        }
      }
      return res;
    }
  }

  private UnnamedColumn generateCaseCondition(
      List<Integer> tierlist,
      Map<Integer, String> multipleTierTableTierInfo) {

    Optional<ColumnOp> col = Optional.absent();
    for (Map.Entry<Integer, String> entry : multipleTierTableTierInfo.entrySet()) {
      BaseColumn tierColumn = new BaseColumn(INNER_RAW_AGG_TABLE_ALIAS, entry.getValue());
      ColumnOp equation =
          new ColumnOp(
              "equal",
              Arrays.asList(tierColumn, ConstantColumn.valueOf(tierlist.get(entry.getKey()))));
      if (col.isPresent()) {
        col = Optional.of(new ColumnOp("and", Arrays.<UnnamedColumn>asList(equation, col.get())));
      } else {
        col = Optional.of(equation);
      }
    }
    return col.get();
  }

  /**
   * Replace the scaled select list with original select list
   *
   * @param queryToReplace, aggMeta
   * @return replaced original select list
   */
  private SelectQuery replaceWithOriginalSelectList(SelectQuery queryToReplace, AggMeta aggMeta) {
    List<SelectItem> originalSelectList = aggMeta.getOriginalSelectList();
    Map<SelectItem, List<ColumnOp>> aggColumn = aggMeta.getAggColumn();
    HashMap<String, UnnamedColumn> aggContents = new HashMap<>();
    for (SelectItem sel : queryToReplace.getSelectList()) {
      // this column is a basic aggregate column
      if (sel instanceof AliasedColumn
          && aggMeta.getAggAlias().contains(((AliasedColumn) sel).getAliasName())) {
        aggContents.put(((AliasedColumn) sel).getAliasName(), ((AliasedColumn) sel).getColumn());
      } else if (sel instanceof AliasedColumn
          && aggMeta.getMaxminAggAlias().keySet().contains(((AliasedColumn) sel).getAliasName())) {
        aggContents.put(((AliasedColumn) sel).getAliasName(), ((AliasedColumn) sel).getColumn());
      }
    }

    for (SelectItem sel : originalSelectList) {
      // Case 1: aggregate column
      if (aggColumn.containsKey(sel)) {
        List<ColumnOp> columnOps = aggColumn.get(sel);
        for (ColumnOp col : columnOps) {
          // If it is count or sum, set col to be aggContents
          if (col.getOpType().equals("count") || col.getOpType().equals("sum")) {
            String aliasName;
            if (col.getOpType().equals("count")) {
              aliasName =
                  aggMeta
                  .getAggColumnAggAliasPair()
                  .get(
                      new ImmutablePair<>(
                          col.getOpType(), (UnnamedColumn) new AsteriskColumn()));
            } else
              aliasName =
              aggMeta
              .getAggColumnAggAliasPair()
              .get(new ImmutablePair<>(col.getOpType(), col.getOperand(0)));
            ColumnOp aggContent = (ColumnOp) aggContents.get(aliasName);
            col.setOpType(aggContent.getOpType());
            col.setOperand(aggContent.getOperands());
          } else if (col.getOpType().equals("max") || col.getOpType().equals("min")) {
            String aliasName =
                aggMeta
                .getAggColumnAggAliasPairOfMaxMin()
                .get(new ImmutablePair<>(col.getOpType(), col.getOperand(0)));
            if (aggContents.get(aliasName) instanceof BaseColumn) {
              BaseColumn aggContent = (BaseColumn) aggContents.get(aliasName);
              col.setOpType("multiply");
              col.setOperand(Arrays.asList(ConstantColumn.valueOf(1), aggContent));
            } else {
              ColumnOp aggContent = (ColumnOp) aggContents.get(aliasName);
              col.setOpType(aggContent.getOpType());
              col.setOperand(aggContent.getOperands());
            }
          }
          // If it is avg, set col to be divide columnOp
          else if (col.getOpType().equals("avg")) {
            String aliasNameSum =
                aggMeta
                .getAggColumnAggAliasPair()
                .get(new ImmutablePair<>("sum", col.getOperand(0)));
            ColumnOp aggContentSum = (ColumnOp) aggContents.get(aliasNameSum);
            String aliasNameCount =
                aggMeta
                .getAggColumnAggAliasPair()
                .get(new ImmutablePair<>("count", (UnnamedColumn) new AsteriskColumn()));
            ColumnOp aggContentCount = (ColumnOp) aggContents.get(aliasNameCount);
            col.setOpType("divide");
            col.setOperand(Arrays.<UnnamedColumn>asList(aggContentSum, aggContentCount));
          }
        }
      }
      // Case 2: non-aggregate column
      // this non-aggregate column must exist in the column list of the scaled table
      else if (sel instanceof AliasedColumn) {
        ((AliasedColumn) sel)
        .setColumn(
            new BaseColumn(TIER_CONSOLIDATED_TABLE_ALIAS, ((AliasedColumn) sel).getAliasName()));
        ((AliasedColumn) sel).setAliasName(((AliasedColumn) sel).getAliasName());
      }
    }
    queryToReplace.clearSelectList();
    queryToReplace.getSelectList().addAll(originalSelectList);
    return queryToReplace;
  }

  /**
   * Create a sum-up query that sum the results from all tier permutations
   *
   * @param subquery
   * @return select query that sum all the tier results
   */
  private static SelectQuery sumUpTierGroup(
      SelectQuery subquery,
      AggMeta sourceAggMeta) {

    List<String> aggAlias = sourceAggMeta.getAggAlias();
    Set<String> tierColumnAliases = sourceAggMeta.getAllTierColumnAliases();

    List<String> groupby = new ArrayList<>();
    List<SelectItem> newSelectlist = new ArrayList<>();
    for (SelectItem sel : subquery.getSelectList()) {
      if (sel instanceof AliasedColumn) {
        // If this is a basic aggregation, we need to sum up
        if (aggAlias.contains(((AliasedColumn) sel).getAliasName())) {
          newSelectlist.add(
              new AliasedColumn(
                  ColumnOp.sum(new BaseColumn(TIER_CONSOLIDATED_TABLE_ALIAS, ((AliasedColumn) sel).getAliasName())),
                  ((AliasedColumn) sel).getAliasName()));
        }
        // If it is a max/min aggregation, we need to maximize/minimize
        else if (sourceAggMeta
            .getMaxminAggAlias()
            .keySet()
            .contains(((AliasedColumn) sel).getAliasName())) {
          String opType = sourceAggMeta.getMaxminAggAlias().get(((AliasedColumn) sel).getAliasName());
          newSelectlist.add(
              new AliasedColumn(
                  new ColumnOp(
                      opType,
                      new BaseColumn(TIER_CONSOLIDATED_TABLE_ALIAS, ((AliasedColumn) sel).getAliasName())),
                  ((AliasedColumn) sel).getAliasName()));
        } else {
          // if it is not a tier column, we need to put it in the group by list
          if (!tierColumnAliases.contains(((AliasedColumn) sel).getAliasName())) {
            newSelectlist.add(
                new AliasedColumn(
                    new BaseColumn(TIER_CONSOLIDATED_TABLE_ALIAS, ((AliasedColumn) sel).getAliasName()),
                    ((AliasedColumn) sel).getAliasName()));
            groupby.add(((AliasedColumn) sel).getAliasName());
          }
        }
      }
    }
    subquery.setAliasName(TIER_CONSOLIDATED_TABLE_ALIAS);
    SelectQuery query = SelectQuery.create(newSelectlist, subquery);
    for (String group : groupby) {
      query.addGroupby(new AliasReference(group));
    }
    return query;
  }

  @Override
  public ExecutableNodeBase deepcopy() {
    AsyncAggExecutionNode copy = new AsyncAggExecutionNode(getNamer());
    copyFields(this, copy);
    return copy;
  }

  void copyFields(AsyncAggExecutionNode from, AsyncAggExecutionNode to) {
    //    to.scrambleMeta = from.scrambleMeta;
    //    to.nonaggColumns = from.nonaggColumns;
    //    to.aggColumns = from.aggColumns;
  }
}
