/*
 * Copyright 2018 University of Michigan
 *
 * You must contact Barzan Mozafari (mozafari@umich.edu) or Yongjoo Park (pyongjoo@umich.edu) to discuss
 * how you could use, modify, or distribute this code. By default, this code is not open-sourced and we do
 * not license this code.
 */

package org.verdictdb.core.querying.ola;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.querying.AggExecutionNode;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.IdCreator;
import org.verdictdb.core.querying.ProjectionNode;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.rewriter.aggresult.AggNameAndType;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaForTable;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

import com.google.common.base.Optional;

/**
 * Represents an "progressive" execution of a single aggregate query (without nested components).
 * <p>
 * Steps:
 * 1. identify agg and nonagg columns of a given select agg query.
 * 2. convert the query into multiple block-agg queries.
 * 3. issue those block-agg queries one by one.
 * 4. combine the results of those block-agg queries as the answers to those queries arrive.
 * 5. depending on the interface, call an appropriate result handler.
 *
 * @author Yongjoo Park
 */
public class AsyncAggExecutionNode extends ProjectionNode {

  // TODO: should be removed
  String tierColumn = "verdictdbtier";

  ScrambleMeta scrambleMeta;

  // group-by columns
  List<String> nonaggColumns;
  //  
  // agg columns. pairs of their column names and their types (i.e., sum, avg, count)
  List<AggNameAndType> aggColumns;

  SelectQuery originalQuery;

//  List<AsyncAggExecutionNode> children = new ArrayList<>();

  int tableNum = 1;

  ExecutionInfoToken savedToken = null;

  // Key is the index of scramble table in Dimension, value is the tier column alias name
  // Only record tables have multiple tiers
  HashMap<Integer, String> multipleTierTableTierInfo = new HashMap<>();

  Boolean tierInfoInitiated = false;

  private AsyncAggExecutionNode() {
    super(null, null);
  }

  public static AsyncAggExecutionNode create(
      IdCreator idCreator,
      List<ExecutableNodeBase> individualAggs,
      List<ExecutableNodeBase> combiners, 
      ScrambleMeta meta) throws VerdictDBValueException {
    
    AsyncAggExecutionNode node = new AsyncAggExecutionNode();

    // first agg -> root
    node.subscribeTo(individualAggs.get(0), 0);

    // combiners -> root
    for (ExecutableNodeBase c : combiners) {
      node.subscribeTo(c, 0);
    }
    
    node.setScrambleMeta(meta);
    return node;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    savedToken = tokens.get(0);

    // First, calculate the scale factor
    List<HyperTableCube> cubes = (List<HyperTableCube>) savedToken.getValue("hyperTableCube");
    HashMap<List<Integer>, Double> scaleFactor = calculateScaleFactor(cubes);

    // Next, create the base select query for replacement
    Pair<List<ColumnOp>, SqlConvertible> aggColumnsAndQuery = createBaseQueryForReplacement(cubes);

    // single tier case multiple tier
    if (scaleFactor.size() == 1) {
      // Substitute the scale factor
      Double s = (Double) (scaleFactor.values().toArray())[0];
      for (ColumnOp col : aggColumnsAndQuery.getLeft()) {
        col.setOperand(0, ConstantColumn.valueOf(s));
      }
    }
    // multiple tiers case
    else {
      // If it has multiple tiers, we need to rewrite the multiply column into when-then-else column
      for (ColumnOp col : aggColumnsAndQuery.getLeft()) {
        col.setOpType("whenthenelse");
        List<UnnamedColumn> operands = new ArrayList<>();
        for (Map.Entry<List<Integer>, Double> entry : scaleFactor.entrySet()) {
          UnnamedColumn condition = generateCaseCondition(entry.getKey());
          operands.add(condition);
          ColumnOp multiply = new ColumnOp("multiply", 
              Arrays.asList(ConstantColumn.valueOf(entry.getValue()),
              col.getOperand(1)));
          operands.add(multiply);
        }
        operands.add(ConstantColumn.valueOf(0));
        col.setOperand(operands);
      }
    }

    return aggColumnsAndQuery.getRight();
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    // TODO: where is the name of the table?
    return savedToken;
  }

  @Override
  public ExecutableNodeBase deepcopy() {
    AsyncAggExecutionNode copy = new AsyncAggExecutionNode();
    copyFields(this, copy);
    return copy;
  }

  void copyFields(AsyncAggExecutionNode from, AsyncAggExecutionNode to) {
    to.scrambleMeta = from.scrambleMeta;
    to.nonaggColumns = from.nonaggColumns;
    to.aggColumns = from.aggColumns;
  }

  public ScrambleMeta getScrambleMeta() {
    return scrambleMeta;
  }

  public void setScrambleMeta(ScrambleMeta meta) {
    this.scrambleMeta = meta;
  }

  /**
   * 
   * @param cubes
   * @return Key: aggregate column list
   */
  Pair<List<ColumnOp>, SqlConvertible> createBaseQueryForReplacement(List<HyperTableCube> cubes) {
    List<ColumnOp> aggColumnlist = new ArrayList<>();
    QueryNodeBase dependent = (QueryNodeBase) savedToken.getValue("dependent");
    List<SelectItem> newSelectList = dependent.getSelectQuery().deepcopy().getSelectList();
    
    if (dependent instanceof AggExecutionNode) {
      for (SelectItem selectItem : newSelectList) {
        // invariant: the agg column must be aliased column
        if (selectItem instanceof AliasedColumn) {
          int index = newSelectList.indexOf(selectItem);
          UnnamedColumn col = ((AliasedColumn) selectItem).getColumn();
          if (AsyncAggExecutionNode.isAggregateColumn(col)) {
            ColumnOp aggColumn = new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                ConstantColumn.valueOf(1.0), new BaseColumn("to_scale_query", ((AliasedColumn) selectItem).getAliasName())
            ));
            aggColumnlist.add(aggColumn);
            newSelectList.set(index, new AliasedColumn(aggColumn, ((AliasedColumn) selectItem).getAliasName()));
          } else {
            // Looking for tier column
            if (col instanceof BaseColumn && ((BaseColumn) col).getColumnName().equals(tierColumn)) {
              String schemaName = ((BaseColumn) col).getSchemaName();
              String tableName = ((BaseColumn) col).getTableName();
              for (Dimension d:cubes.get(0).getDimensions()) {
                if (d.getTableName().equals(tableName)&&d.getSchemaName().equals(schemaName)) {
                  multipleTierTableTierInfo.put(cubes.get(0).getDimensions().indexOf(d), ((AliasedColumn) selectItem).getAliasName());
                  break;
                }
              }
            }
            newSelectList.set(index, new AliasedColumn(new BaseColumn("to_scale_query", ((AliasedColumn) selectItem).getAliasName()),
                ((AliasedColumn) selectItem).getAliasName()));
          }
        }
      }
      tierInfoInitiated = true;
    } 
    else if (dependent instanceof AggCombinerExecutionNode) {
      for (SelectItem selectItem : newSelectList) {
        if (selectItem instanceof AliasedColumn) {
          int index = newSelectList.indexOf(selectItem);
          
          // TODO: what is this?
          // Let's do this:
          // 1. extend AggExecutionNode to create the "OlaIndividualAggNode" class
          // 2. Let it create/stores/propagates "cubes" and "(alias name, agg type)"
          if (((AliasedColumn) selectItem).getAliasName().matches("s[0-9]+") || ((AliasedColumn) selectItem).getAliasName().matches("c[0-9]+")) {
            ColumnOp aggColumn = new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                ConstantColumn.valueOf(1.0), new BaseColumn("to_scale_query", ((AliasedColumn) selectItem).getAliasName())
            ));
            aggColumnlist.add(aggColumn);
            newSelectList.set(index, new AliasedColumn(aggColumn, ((AliasedColumn) selectItem).getAliasName()));
          } else {
            newSelectList.set(index, new AliasedColumn(new BaseColumn("to_scale_query", ((AliasedColumn) selectItem).getAliasName()),
                ((AliasedColumn) selectItem).getAliasName()));
          }
        }
      }
    }
    // Setup from table
    SelectQuery query = SelectQuery.create(newSelectList,
        new BaseTable((String) savedToken.getValue("schemaName"), (String) savedToken.getValue("tableName"), "to_scale_query"));
    return new ImmutablePair<>(aggColumnlist, (SqlConvertible) query);
  }

  /**
   *  Currently, assume block size is uniform
   *  @return Return tier permutation list and its scale factor
   */
  // TODO: could we distinguish different tier column names of different tables?
  public HashMap<List<Integer>, Double> calculateScaleFactor(List<HyperTableCube> cubes) {
    
    ScrambleMeta scrambleMeta = this.getScrambleMeta();
    List<ScrambleMetaForTable> metaForTablesList = new ArrayList<>();
    List<Integer> blockCountList = new ArrayList<>();
    List<Pair<Integer, Integer>> scrambleTableTierInfo = new ArrayList<>();
    
    for (Dimension d : cubes.get(0).getDimensions()) {
      blockCountList.add(scrambleMeta.getAggregationBlockCount(d.getSchemaName(), d.getTableName()));
      metaForTablesList.add(scrambleMeta.getMetaForTable(d.getSchemaName(), d.getTableName()));
      scrambleTableTierInfo.add(
          new ImmutablePair<>(cubes.get(0).getDimensions().indexOf(d),
          scrambleMeta.getMetaForTable(d.getSchemaName(), d.getTableName()).getNumberOfTiers()));
      if (scrambleMeta.getMetaForTable(d.getSchemaName(), d.getTableName()).getNumberOfTiers() > 1 && !tierInfoInitiated) {
        multipleTierTableTierInfo.put(cubes.get(0).getDimensions().indexOf(d),
            scrambleMeta.getMetaForTable(d.getSchemaName(), d.getTableName()).getTierColumn());
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
//          double prob = d.getBegin() == 0 ? metaForTablesList.get(i).getCumulativeProbabilityDistribution(tier).get(d.getEnd())
//              : metaForTablesList.get(i).getCumulativeProbabilityDistribution(tier).get(d.getEnd()) -
//              metaForTablesList.get(i).getCumulativeProbabilityDistribution(tier).get(d.getBegin() - 1);
          double prob = 0;
          if (d.getBegin() == 0) {
            prob = metaForTablesList.get(i).getCumulativeProbabilityDistribution(tier).get(d.getEnd());
          } else {
            prob = metaForTablesList.get(i).getCumulativeProbabilityDistribution(tier).get(d.getEnd())
                - metaForTablesList.get(i).getCumulativeProbabilityDistribution(tier).get(d.getBegin() - 1);
          }
          
          scale = scale * prob;
        }
        total += scale;
      }
      if (total == 0) scaleFactor.put(tierlist, 0.0);
      else scaleFactor.put(tierlist, 1 / total);
    }
    return scaleFactor;
  }

  /** 
   * Generate the permuation of multiple tiers
   * @param scrambleTableTierInfo
   * @return
   */
  List<List<Integer>> generateTierPermuation(List<Pair<Integer, Integer>> scrambleTableTierInfo) {
    if (scrambleTableTierInfo.size() == 1) {
      List<List<Integer>> res = new ArrayList<>();
      for (int tier = 0; tier < scrambleTableTierInfo.get(0).getRight(); tier++) {
        res.add(Arrays.asList(tier));
      }
      return res;
    } else {
      List<Pair<Integer, Integer>> next = scrambleTableTierInfo.subList(1, scrambleTableTierInfo.size());
      List<List<Integer>> subres = generateTierPermuation(next);
      List<List<Integer>> res = new ArrayList<>();
      for (int tier = 0; tier < scrambleTableTierInfo.get(0).getRight(); tier++) {
        for (List<Integer> tierlist : subres) {
          List<Integer> newTierlist = new ArrayList<>();
          for (int i:tierlist) {
            newTierlist.add(Integer.valueOf(i));
          }
          newTierlist.add(0, tier);
          res.add(newTierlist);
        }
      }
      return res;
    }
  }

  UnnamedColumn generateCaseCondition(List<Integer> tierlist) {
    Optional<ColumnOp> col = Optional.absent();
    for (Map.Entry<Integer, String> entry:multipleTierTableTierInfo.entrySet()) {
      BaseColumn tierColumn = new BaseColumn("to_scale_query", entry.getValue());
      ColumnOp equation = new ColumnOp("equal", Arrays.asList(tierColumn,
          ConstantColumn.valueOf(tierlist.get(entry.getKey()))));
      if (col.isPresent()) {
        col = Optional.of(new ColumnOp("equal", Arrays.<UnnamedColumn>asList(equation, col.get())));
      }
      else col = Optional.of(equation);
    }
    return col.get();
  }

  // Currently, only need to judge whether it is sum or count
  public static boolean isAggregateColumn(UnnamedColumn sel) {
    List<SelectItem> itemToCheck = new ArrayList<>();
    itemToCheck.add(sel);
    while (!itemToCheck.isEmpty()) {
      SelectItem s = itemToCheck.get(0);
      itemToCheck.remove(0);
      if (s instanceof ColumnOp) {
        if (((ColumnOp) s).getOpType().equals("count") || ((ColumnOp) s).getOpType().equals("sum")) {
          return true;
        }
        else itemToCheck.addAll(((ColumnOp) s).getOperands());
      }
    }
    return false;
  }
}
