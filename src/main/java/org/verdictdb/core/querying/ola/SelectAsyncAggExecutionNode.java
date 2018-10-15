package org.verdictdb.core.querying.ola;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.HashDbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.*;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

import java.util.*;

public class SelectAsyncAggExecutionNode extends QueryNodeBase {

  private static final long serialVersionUID = 70795390245860583L;

  private static final String HAVING_CONDITION_ALIAS = "verdictdb_having_cond";

  private static final String GROUP_BY_ALIAS = "verdictdb_group_by";

  private static final String ORDER_BY_ALIAS = "verdictdb_order_by";

  private DbmsQueryResult dbmsQueryResult;

  private ScrambleMetaSet scrambleMeta;

  private HashMap<List<Integer>, Double> conditionToScaleFactorMap = new HashMap<>();

  private Map<Integer, String> scrambledTableTierInfo;

  /**
   * This is the Map that maps the aggregation alias to its column contents. For example, if one
   * basic aggregate column in the aggregate node is sum(value) as agg0, it will record [agg0,
   * sum(value)].
   */
  HashMap<String, UnnamedColumn> aggContents = new HashMap<>();

  private SelectAsyncAggExecutionNode(IdCreator idCreator) {
    super(idCreator, null);
  }

  /**
   * A factory method for AsyncAggExecutionNode.
   *
   * <p>This static method performs the following operations: 1. Link individual aggregate nodes and
   * combiners appropriately. 2. Create a base table which includes several placeholders - scale
   * factor placeholder - temp table placeholder
   *
   * @param idCreator
   * @param selectAggs
   * @param meta
   * @param aggNodeBlock
   * @return
   */

  public static SelectAsyncAggExecutionNode create(
      IdCreator idCreator,
      List<SelectAggExecutionNode> selectAggs,
      ScrambleMetaSet meta,
      AggExecutionNodeBlock aggNodeBlock) {
    SelectAsyncAggExecutionNode node = new SelectAsyncAggExecutionNode(idCreator);

    // individual aggs -> root
    SubscriptionTicket ticket = node.createSubscriptionTicket();
    for (SelectAggExecutionNode agg:selectAggs) {
      agg.registerSubscriber(ticket);
    }

    node.setScrambleMetaSet(meta); // the scramble meta must be not be shared; thus, thread-safe

    // creates a base query that contain placeholders
    AggMeta sourceAggMeta = selectAggs.get(0).getAggMeta();
    List<SelectItem> sourceSelectList = selectAggs.get(0).getSelectQuery().getSelectList();
    Triple<List<ColumnOp>, List<SelectItem>, Map<Integer, String>> aggColumnsAndQuery =
        createBaseQueryForReplacement(sourceAggMeta, sourceSelectList, meta);
    SelectQuery subquery = SelectQuery.create(aggColumnsAndQuery.getMiddle(), new BaseTable("dummy"));
    Pair<SelectQuery, HashMap<String, UnnamedColumn>> pair =
        sumUpTierGroup(subquery, sourceAggMeta);
    node.selectQuery = pair.getLeft();
    node.aggContents = pair.getRight();
    node.scrambledTableTierInfo =
        new ImmutableMap.Builder<Integer, String>().putAll(aggColumnsAndQuery.getRight()).build();

    // add (1) order-by, (2) limit, (3) having clauses to the select query
    QueryNodeBase aggRoot = (QueryNodeBase) aggNodeBlock.getBlockRootNode();
    SelectQuery originalAggQuery = aggRoot.getSelectQuery();
    node.selectQuery.addOrderby(originalAggQuery.getOrderby());
    if (originalAggQuery.getLimit().isPresent()) {
      node.selectQuery.addLimit(originalAggQuery.getLimit().get());
    }
    if (originalAggQuery.getHaving().isPresent()) {
      node.selectQuery.addHavingByAnd(originalAggQuery.getHaving().get());
    }

    node.selectQuery = node.replaceWithOriginalSelectList(node.selectQuery, sourceAggMeta);

    // set up HashdbmsQueryResult
    node.dbmsQueryResult = new HashDbmsQueryResult(node);
    List<Integer> isAggregate = new ArrayList<Integer>();
    for (SelectItem sel:sourceSelectList) {
      if (sel instanceof AliasedColumn && ((AliasedColumn) sel).getAliasName().matches("agg[0-9]+$")) {
        ColumnOp columnOp = (ColumnOp) ((AliasedColumn) sel).getColumn();
        if (columnOp.getOpType().equals("max")) {
          isAggregate.add(2);
        } else if (columnOp.getOpType().equals("min")) {
          isAggregate.add(3);
        } else {
          isAggregate.add(1);
        }
      } else {
        isAggregate.add(0);
      }
    }
    ((HashDbmsQueryResult) node.dbmsQueryResult).setIsAggregate(isAggregate);
    List<Integer> tierColumnIndex = new ArrayList<>();
    for (String colName:node.scrambledTableTierInfo.values()) {
      for (SelectItem sel:sourceSelectList) {
        if (sel instanceof AliasedColumn && ((AliasedColumn) sel).getAliasName().equals(colName)) {
          tierColumnIndex.add(sourceSelectList.indexOf(sel));
        }
      }
    }
    ((HashDbmsQueryResult) node.dbmsQueryResult).setTierInfo(tierColumnIndex);

    return node;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    ExecutionInfoToken token = tokens.get(0);
    AggMeta sourceAggMeta = (AggMeta) token.getValue("aggMeta");

    // Update scale factor
    List<Pair<UnnamedColumn, Double>> conditionToScaleFactor = composeScaleFactorForTierCombinations(sourceAggMeta);
    for (Pair<UnnamedColumn, Double> pair:conditionToScaleFactor) {
      List<Integer> tierCondition = convertTierConditionColToList((ColumnOp)pair.getLeft());
      if (!conditionToScaleFactorMap.containsKey(tierCondition)) {
        conditionToScaleFactorMap.put(tierCondition, pair.getRight());
      } else {
        double scale = conditionToScaleFactorMap.get(tierCondition);
        conditionToScaleFactorMap.put(tierCondition, 1.0/(1.0/scale + 1.0/pair.getRight()));
      }
    }


    // update dbmsQueryResult
    ((HashDbmsQueryResult)dbmsQueryResult).addDbmsQueryResult((DbmsQueryResult) token.getValue("queryResult"), conditionToScaleFactorMap, this);
    return null;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = super.createToken(result);
    token.setKeyValue("queryResult", dbmsQueryResult);
    return token;
  }

  private List<Integer> convertTierConditionColToList(ColumnOp condition) {
    List<Integer> condList = new ArrayList<>();
    if (condition.getOpType().equals("equal")) {
      condList.add((Integer) ((ConstantColumn)condition.getOperand(1)).getValue());
    } else if (condition.getOpType().equals("and")) {
      condList.addAll(convertTierConditionColToList((ColumnOp) condition.getOperand(0)));
      condList.addAll(convertTierConditionColToList((ColumnOp) condition.getOperand(1)));
    }
    return condList;
  }

  /**
   * Compose pairs of tier indicator and its associated scaling factor. The function generates the
   * operands needed for writing (by the caller) the following case-when clause: "case when cond1
   * then scale1 when cond2 else scale2 end" clause.
   *
   * @param sourceAggMeta The aggmeta of the downstream node
   * @return Map of a filtering condition to the scaling factor; the filtering conditions correspond
   *     to cond1, cond2, etc.; the scaling factors correspond to scale1, scale2, etc.
   */
  private List<Pair<UnnamedColumn, Double>> composeScaleFactorForTierCombinations(
      AggMeta sourceAggMeta) {
    List<Pair<UnnamedColumn, Double>> scalingFactorPerTier = new ArrayList<>();

    Map<TierCombination, Double> scaleFactors = sourceAggMeta.computeScaleFactors();
    Map<ScrambleMeta, String> tierColums = sourceAggMeta.getTierColumnForScramble();

    // each iteration of this loop generates a single condition-then part
    for (Map.Entry<TierCombination, Double> tierScale : scaleFactors.entrySet()) {
      UnnamedColumn tierCombinationCondition = null;
      TierCombination combination = tierScale.getKey();
      double scale = tierScale.getValue();

      // each iteration of this loop generates a condition that must be AND-connected
      // to compose the condition for an entire tier combination.
      for (Map.Entry<Pair<String, String>, Integer> perTable : combination) {
        Pair<String, String> table = perTable.getKey();
        Integer tier = perTable.getValue();
        String aliasName = findScrambleAlias(tierColums, table);

        UnnamedColumn part =
            ColumnOp.equal(
                new BaseColumn("", aliasName), ConstantColumn.valueOf(tier));

        if (tierCombinationCondition == null) {
          tierCombinationCondition = part;
        } else {
          tierCombinationCondition = ColumnOp.and(part, tierCombinationCondition);
        }
      }

      scalingFactorPerTier.add(new ImmutablePair<>(tierCombinationCondition, scale));
    }

    return scalingFactorPerTier;
  }

  private String findScrambleAlias(
      Map<ScrambleMeta, String> tierColums, Pair<String, String> table) {

    for (Map.Entry<ScrambleMeta, String> metaToAlias : tierColums.entrySet()) {
      ScrambleMeta meta = metaToAlias.getKey();
      String aliasName = metaToAlias.getValue();
      if (meta.getSchemaName().equals(table.getLeft())
          && meta.getTableName().equals(table.getRight())) {
        return aliasName;
      }
    }
    return null;
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
   *     aggregate node or a combiner node). This object contains what are the tier columns for the
   *     scrambled tables they cover and what aggregates are being computed.
   * @return Key: aggregate column list
   */
  private static Triple<List<ColumnOp>, List<SelectItem>, Map<Integer, String>>
  createBaseQueryForReplacement(
      AggMeta sourceAggMeta,
      List<SelectItem> sourceSelectList,
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
                  new BaseColumn("", aliasedColumn.getAliasName()));
          aggColumnlist.add(aggColumn);
          newSelectList.set(index, new AliasedColumn(aggColumn, aliasedColumn.getAliasName()));
        } else if (sourceAggMeta
            .getMaxminAggAlias()
            .keySet()
            .contains(aliasedColumn.getAliasName())) {
          newSelectList.set(
              index,
              new AliasedColumn(
                  new BaseColumn("", aliasedColumn.getAliasName()),
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
                  new BaseColumn("", aliasedColumn.getAliasName()),
                  aliasedColumn.getAliasName()));
        }
      }
    }

    return Triple.of(aggColumnlist, newSelectList, multipleTierTableTierInfo);
  }

  /**
   * Currently, assume block size is uniform
   *
   * @return Return tier permutation list and its scale factor
   */
  private HashMap<List<Integer>, Double> calculateScaleFactor(
      AggMeta sourceAggMeta, Map<Integer, String> multipleTierTableTierInfo)
      throws VerdictDBValueException {

    List<HyperTableCube> cubes = sourceAggMeta.getCubes();
    ScrambleMetaSet scrambleMetaSet = this.getScrambleMeta();
    List<ScrambleMeta> metaForTablesList = new ArrayList<>();
    List<Integer> blockCountList = new ArrayList<>();
    List<Pair<Integer, Integer>> scrambleTableTierInfo =
        new ArrayList<>(); // block span index for each table

    for (Dimension d : cubes.get(0).getDimensions()) {
      ScrambleMeta scrambleMeta =
          scrambleMetaSet.getSingleMeta(d.getSchemaName(), d.getTableName());

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
        //        ScrambleMeta meta = scrambleMetaSet.getSingleMeta(d.getSchemaName(),
        // d.getTableName());
        Map<ScrambleMeta, String> scrambleTableTierColumnAlias =
            sourceAggMeta.getTierColumnForScramble();

        if (scrambleTableTierColumnAlias.containsKey(scrambleMeta) == false) {
          throw new VerdictDBValueException("The metadata for a scrambled table is not found.");
        }

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
            prob =
                metaForTablesList.get(i).getCumulativeDistributionForTier(tier).get(d.getEnd())
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
  private List<List<Integer>> generateTierPermuation(
      List<Pair<Integer, Integer>> scrambleTableTierInfo) {
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

  private SelectItem replaceColumnWithAggMeta(SelectItem sel, AggMeta aggMeta) {

    Map<SelectItem, List<ColumnOp>> aggColumn = aggMeta.getAggColumn();
    // Case 1: aggregate column
    if (aggColumn.containsKey(sel)) {
      List<ColumnOp> columnOps = aggColumn.get(sel);
      for (ColumnOp col : columnOps) {
        // If it is count or sum, set col to be aggContents
        if (col.getOpType().equals("count") || col.getOpType().equals("sum")) {
          String aliasName = null;
          if (col.getOpType().equals("count")) {
            aliasName =
                aggMeta
                    .getAggColumnAggAliasPair()
                    .get(
                        new ImmutablePair<>(col.getOpType(), (UnnamedColumn) new AsteriskColumn()));
            if (aliasName==null) {
              aliasName =
                  aggMeta
                      .getAggColumnAggAliasPair()
                      .get(new ImmutablePair<>(col.getOpType(), col.getOperand(0)));
            }
          } else {
            aliasName =
                aggMeta
                    .getAggColumnAggAliasPair()
                    .get(new ImmutablePair<>(col.getOpType(), col.getOperand(0)));
          }
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
              aggMeta.getAggColumnAggAliasPair().get(new ImmutablePair<>("sum", col.getOperand(0)));
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
      //      AliasedColumn ac = (AliasedColumn) sel;
      //      if (ac.getAliasName().startsWith(AsyncAggExecutionNode.getHavingConditionAlias())
      //          || ac.getAliasName().startsWith(AsyncAggExecutionNode.getGroupByAlias())
      //          || ac.getAliasName().startsWith(AsyncAggExecutionNode.getOrderByAlias())) {
      //        return null;
      //      } else {
      ((AliasedColumn) sel)
          .setColumn(
              new BaseColumn("", ((AliasedColumn) sel).getAliasName()));
      ((AliasedColumn) sel).setAliasName(((AliasedColumn) sel).getAliasName());
      //      }
    }
    return sel;
  }

  /**
   * Replace the scaled select list with original select list
   *
   * @param queryToReplace, aggMeta
   * @return replaced original select list
   */
  private SelectQuery replaceWithOriginalSelectList(SelectQuery queryToReplace, AggMeta aggMeta) {
    List<SelectItem> originalSelectList = aggMeta.getOriginalSelectList();
    List<SelectItem> newSelectList = new ArrayList<>();

    boolean firstHaving = true;
    int orderByIndex = 0;
    for (SelectItem sel : originalSelectList) {
      SelectItem replacedSel = this.replaceColumnWithAggMeta(sel, aggMeta);
      SelectItem item = (replacedSel != null) ? replacedSel : sel;
      if (item instanceof AliasedColumn) {
        AliasedColumn ac = (AliasedColumn) item;
        if (ac.getAliasName().startsWith(HAVING_CONDITION_ALIAS)) {
          if (firstHaving) {
            queryToReplace.clearHaving();
            firstHaving = false;
          }
          queryToReplace.addHavingByAnd(ac.getColumn());
        } else if (ac.getAliasName().startsWith(ORDER_BY_ALIAS)) {
          OrderbyAttribute attribute = queryToReplace.getOrderby().get(orderByIndex++);
          attribute.setAttribute(ac.getColumn());
        }
        if (ac.getAliasName().startsWith(HAVING_CONDITION_ALIAS)
            || ac.getAliasName().startsWith(ORDER_BY_ALIAS)
            || ac.getAliasName().startsWith(GROUP_BY_ALIAS)) {
          continue;
        }
      }
      newSelectList.add(item);
    }
    queryToReplace.clearSelectList();
    queryToReplace.getSelectList().addAll(newSelectList);
    return queryToReplace;
  }

  /**
   * Create a sum-up query that sum the results from all tier permutations
   *
   * @param subquery
   * @return select a pair that key is the query that sum all the tier results and value is the
   *     aggContents that record the aggregation column alias and the column itself
   */
  private static Pair<SelectQuery, HashMap<String, UnnamedColumn>> sumUpTierGroup(
      SelectQuery subquery, AggMeta sourceAggMeta) {

    List<String> aggAlias = sourceAggMeta.getAggAlias();
    Set<String> tierColumnAliases = sourceAggMeta.getAllTierColumnAliases();
    HashMap<String, UnnamedColumn> aggContents = new HashMap<>();

    List<GroupingAttribute> groupby = new ArrayList<>();
    List<SelectItem> newSelectlist = new ArrayList<>();
    for (SelectItem sel : subquery.getSelectList()) {
      if (sel instanceof AliasedColumn) {
        // If this is a basic aggregation, we need to sum up
        if (aggAlias.contains(((AliasedColumn) sel).getAliasName())) {
          UnnamedColumn col =
              ColumnOp.sum(
                  new BaseColumn(
                      "", ((AliasedColumn) sel).getAliasName()));
          newSelectlist.add(new AliasedColumn(col, ((AliasedColumn) sel).getAliasName()));
          aggContents.put(((AliasedColumn) sel).getAliasName(), col);
          String alias = ((AliasedColumn) sel).getAliasName();

          // Add the item to group-by if it is group-by column
          if (alias.startsWith(GROUP_BY_ALIAS)) {
            UnnamedColumn newcol = new BaseColumn("", alias);
            groupby.add(newcol);
          }
        }
        // If it is a max/min aggregation, we need to maximize/minimize
        else if (sourceAggMeta
            .getMaxminAggAlias()
            .keySet()
            .contains(((AliasedColumn) sel).getAliasName())) {
          String opType =
              sourceAggMeta.getMaxminAggAlias().get(((AliasedColumn) sel).getAliasName());
          UnnamedColumn col =
              new ColumnOp(
                  opType,
                  new BaseColumn(
                      "", ((AliasedColumn) sel).getAliasName()));
          newSelectlist.add(new AliasedColumn(col, ((AliasedColumn) sel).getAliasName()));
          aggContents.put(((AliasedColumn) sel).getAliasName(), col);
        } else {
          // if it is not a tier column, we need to put it in the group by list
          if (!tierColumnAliases.contains(((AliasedColumn) sel).getAliasName())) {
            UnnamedColumn newcol =
                new BaseColumn("", ((AliasedColumn) sel).getAliasName());
            groupby.add(newcol);
            AliasedColumn ac = (AliasedColumn) sel;
            if (ac.getAliasName().startsWith(AsyncAggExecutionNode.getHavingConditionAlias())
                || ac.getAliasName().startsWith(AsyncAggExecutionNode.getGroupByAlias())
                || ac.getAliasName().startsWith(AsyncAggExecutionNode.getOrderByAlias())) {
              continue;
            }
            newSelectlist.add(new AliasedColumn(newcol, ((AliasedColumn) sel).getAliasName()));
          }
        }
      }
    }
    subquery.setAliasName("");
    SelectQuery query = SelectQuery.create(newSelectlist, subquery);
    for (GroupingAttribute group : groupby) {
      query.addGroupby(group);
    }
    return new ImmutablePair<>(query, aggContents);
  }

  public static String getHavingConditionAlias() {
    return HAVING_CONDITION_ALIAS;
  }

  public static String getGroupByAlias() {
    return GROUP_BY_ALIAS;
  }

  public static String getOrderByAlias() {
    return ORDER_BY_ALIAS;
  }

  void copyFields(AsyncAggExecutionNode from, AsyncAggExecutionNode to) {
    //    to.scrambleMeta = from.scrambleMeta;
    //    to.nonaggColumns = from.nonaggColumns;
    //    to.aggColumns = from.aggColumns;
  }
}
