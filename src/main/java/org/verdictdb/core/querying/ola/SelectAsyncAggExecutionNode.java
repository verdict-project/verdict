package org.verdictdb.core.querying.ola;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.InMemoryAggregate;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.*;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import java.sql.SQLException;
import java.util.*;


/**
 * An selectAsyncAggExecutionNode will be created when the outer query is an aggregation query
 * that contains the scramble table. Instead of creating AsyncAggExecutionNode,
 * selectAggExecutionNode will be created. The sources of selectAggExecutionNode are
 * selectAggExecutionNode, which will create temporary table in H2 database. Then,
 * selectAsyncAggExecutionNode will use the results from H2 database to calculate the query result.
 *
 * @author Shucheng Zhong
 *
 */
public class SelectAsyncAggExecutionNode extends AsyncAggExecutionNode {

  private static final long serialVersionUID = 70795390245860583L;

  private DbmsQueryResult dbmsQueryResult;

  private String selectAsyncAggTableName = "";

  // The key of this map is a list of tier numbers (e.g., [1, 2]),
  // and the value of this map is the corresponding scale factor (e.g., 10.0);
  // that is, an entry of this map could be [1, 2] -> 10.0
  private HashMap<List<Integer>, Double> conditionToScaleFactorMap = new HashMap<>();

  private Map<Integer, String> scrambledTableTierInfo;

  private AggMeta aggMeta;

  private SelectAsyncAggExecutionNode(IdCreator idCreator) {
    super(idCreator);
  }


  /**
   * A factory method for SelectAsyncAggExecutionNode.
   *
   * <p>This static method performs the following operations: 1. Link individual selectAggregate nodes
   * 2. Replace the SelectQuery with base aggregation and create a InMemoryAggregate object
   *
   * @param idCreator
   * @param selectAggs
   * @param meta
   * @param aggNodeBlock
   * @return
   */
  public static SelectAsyncAggExecutionNode create(
      IdCreator idCreator,
      List<ExecutableNodeBase> selectAggs,
      ScrambleMetaSet meta,
      AggExecutionNodeBlock aggNodeBlock) {
    SelectAsyncAggExecutionNode node = new SelectAsyncAggExecutionNode(idCreator);

    // this placeholder base table is used for query construction later
    Pair<BaseTable, SubscriptionTicket> tableAndTicket = node.createPlaceHolderTable(INNER_RAW_AGG_TABLE_ALIAS);
    BaseTable placeholderTable = tableAndTicket.getLeft();
    SubscriptionTicket ticket = tableAndTicket.getRight();

    // individual aggs -> root
    for (ExecutableNodeBase agg : selectAggs) {
      agg.registerSubscriber(ticket);
    }

    node.setScrambleMetaSet(meta); // the scramble meta must be not be shared; thus, thread-safe
    node.setNamer(idCreator); // the name can be shared

    // creates a base query that contain placeholders
    SelectAggExecutionNode firstSource = (SelectAggExecutionNode) selectAggs.get(0);
    AggMeta sourceAggMeta = firstSource.getAggMeta();
    List<SelectItem> sourceSelectList = firstSource.getSelectQuery().getSelectList();
    Triple<List<ColumnOp>, SqlConvertible, Map<Integer, String>> aggColumnsAndQuery =
        createBaseQueryForReplacement(sourceAggMeta, sourceSelectList, placeholderTable, meta);
    node.aggColumns = aggColumnsAndQuery.getLeft();
    SelectQuery subquery = (SelectQuery) aggColumnsAndQuery.getMiddle();
    Pair<SelectQuery, HashMap<String, UnnamedColumn>> pair =
        sumUpTierGroup(subquery, sourceAggMeta);
    node.selectQuery = pair.getLeft();
    ((AsyncAggExecutionNode) node).aggContents = pair.getRight();
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
    /*
    node.selectQuery = node.replaceWithOriginalSelectList(node.selectQuery, sourceAggMeta);

    // set up HashdbmsQueryResult
    node.dbmsQueryResult = new InMemoryAggregate(node);
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
    ((InMemoryAggregate) node.dbmsQueryResult).setIsAggregate(isAggregate);
    List<Integer> tierColumnIndex = new ArrayList<>();
    for (String colName:node.scrambledTableTierInfo.values()) {
      for (SelectItem sel:sourceSelectList) {
        if (sel instanceof AliasedColumn && ((AliasedColumn) sel).getAliasName().equals(colName)) {
          tierColumnIndex.add(sourceSelectList.indexOf(sel));
        }
      }
    }
    ((InMemoryAggregate) node.dbmsQueryResult).setTierInfo(tierColumnIndex);
    */
    return node;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    ExecutionInfoToken token = tokens.get(0);
    String table = (String) token.getValue("tableName");
    SelectQuery dependentQuery = (SelectQuery) token.getValue("dependentQuery");
    synchronized (this) {
      if (aggMeta==null) {
        aggMeta = (AggMeta) token.getValue("aggMeta");
      } else {
        AggMeta childAggMeta = (AggMeta) token.getValue("aggMeta");
        updateAggMeta(childAggMeta);
        token.setKeyValue("aggMeta", aggMeta);
      }
      try {
        selectAsyncAggTableName = InMemoryAggregate.combinedTableName(table, selectAsyncAggTableName, dependentQuery);
        token.setKeyValue("tableName", selectAsyncAggTableName);
        SelectQuery query = ((CreateTableAsSelectQuery) super.createQuery(tokens)).getSelect();
        dbmsQueryResult = InMemoryAggregate.executeQuery(query);
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    return null;
    /*
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
    String selectAggTableName = (String)token.getValue("tableName");
    ((InMemoryAggregate)dbmsQueryResult).addDbmsQueryResult((DbmsQueryResult) token.getValue("queryResult"), conditionToScaleFactorMap, this);
    return null;
    */
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = super.createToken(result);
    token.setKeyValue("queryResult", dbmsQueryResult);
    return token;
  }

  private void updateAggMeta(AggMeta childAggMeta) {
    List<HyperTableCube> cubes = new ArrayList<>();
    cubes.addAll(aggMeta.getCubes());
    cubes.addAll(childAggMeta.getCubes());
    aggMeta.setCubes(cubes);
    aggMeta.setAggAlias(childAggMeta.getAggAlias());
    aggMeta.setOriginalSelectList(childAggMeta.getOriginalSelectList());
    aggMeta.setAggColumn(childAggMeta.getAggColumn());
    aggMeta.setAggColumnAggAliasPair(childAggMeta.getAggColumnAggAliasPair());
    aggMeta.setAggColumnAggAliasPairOfMaxMin(childAggMeta.getAggColumnAggAliasPairOfMaxMin());
    aggMeta.setMaxminAggAlias(childAggMeta.getMaxminAggAlias());
    aggMeta.setTierColumnForScramble(childAggMeta.getTierColumnForScramble());
  }

  private List<Integer> convertTierConditionColToList(ColumnOp condition) {
    List<Integer> condList = new ArrayList<>();
    if (condition.getOpType().equals("equal")) {
      condList.add((Integer) ((ConstantColumn) condition.getOperand(1)).getValue());
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
   * to cond1, cond2, etc.; the scaling factors correspond to scale1, scale2, etc.
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


}
