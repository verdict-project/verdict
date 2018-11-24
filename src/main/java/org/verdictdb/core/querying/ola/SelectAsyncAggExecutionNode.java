package org.verdictdb.core.querying.ola;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.InMemoryAggregate;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.IdCreator;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.querying.SelectAggExecutionNode;
import org.verdictdb.core.querying.SubscriptionTicket;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;


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
//  private HashMap<List<Integer>, Double> conditionToScaleFactorMap = new HashMap<>();

//  private Map<Integer, String> scrambledTableTierInfo;

//  private AggMeta aggMeta;

  private InMemoryAggregate inMemoryAggregate = InMemoryAggregate.create();

  private SelectAsyncAggExecutionNode(IdCreator idCreator) {
    super(idCreator);
  }


  /**
   * A factory method for SelectAsyncAggExecutionNode.
   *
   * This static method performs the following operations: 
   * 1. Link individual selectAggregate nodes
   * 2. Replace the SelectQuery with base aggregation and create an InMemoryAggregate object
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
    Pair<BaseTable, SubscriptionTicket> tableAndTicket = 
        node.createPlaceHolderTable(INNER_RAW_AGG_TABLE_ALIAS);
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
//    node.scrambledTableTierInfo =
//        new ImmutableMap.Builder<Integer, String>().putAll(aggColumnsAndQuery.getRight()).build();

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

    // share same inMemoryAggregate object with selectAggExecutionNode
    for (ExecutableNodeBase source:node.getSources()) {
      ((SelectAggExecutionNode)source).setInMemoryAggregate(node.inMemoryAggregate);
    }
    return node;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    ExecutionInfoToken token = tokens.get(0);
    String table = (String) token.getValue("tableName");
    SelectQuery dependentQuery = (SelectQuery) token.getValue("dependentQuery");
    
    // In case multiple answers are already ready, we must synchronize them.
    // To achieve this, we put the lock using this class.
    synchronized (SelectAsyncAggExecutionNode.class) {
      if (aggMeta == null) {
        aggMeta = (AggMeta) token.getValue("aggMeta");
      } else {
        AggMeta childAggMeta = (AggMeta) token.getValue("aggMeta");
        updateAggMeta(childAggMeta);
        token.setKeyValue("aggMeta", aggMeta);
      }
      try {
        selectAsyncAggTableName = 
            inMemoryAggregate.combineTables(table, selectAsyncAggTableName, dependentQuery);
        token.setKeyValue("tableName", selectAsyncAggTableName);
        
        // here, the base aggregate functions (e.g., sum(col), count(col)) are composed to
        // reconstruct the original aggregate function (e.g., avg(col) = sum(col) / count(col))
        SelectQuery query = ((CreateTableAsSelectQuery) super.createQuery(tokens)).getSelect();
        dbmsQueryResult = inMemoryAggregate.executeQuery(query);
        
      } catch (SQLException e) {
        throw new VerdictDBDbmsException(e);
//        e.printStackTrace();
      }
    }
    return null;
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

//  private String findScrambleAlias(
//      Map<ScrambleMeta, String> tierColums, Pair<String, String> table) {
//
//    for (Map.Entry<ScrambleMeta, String> metaToAlias : tierColums.entrySet()) {
//      ScrambleMeta meta = metaToAlias.getKey();
//      String aliasName = metaToAlias.getValue();
//      if (meta.getSchemaName().equals(table.getLeft())
//          && meta.getTableName().equals(table.getRight())) {
//        return aliasName;
//      }
//    }
//    return null;
//  }

  public ScrambleMetaSet getScrambleMeta() {
    return scrambleMeta;
  }

  public void setScrambleMetaSet(ScrambleMetaSet meta) {
    this.scrambleMeta = meta;
  }

  public void abort() {
    inMemoryAggregate.abort();
  }
}
