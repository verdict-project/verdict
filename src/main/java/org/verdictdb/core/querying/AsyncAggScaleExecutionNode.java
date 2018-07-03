package org.verdictdb.core.querying;

import java.util.*;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.querying.ola.AsyncAggExecutionNode;
import org.verdictdb.core.querying.ola.Dimension;
import org.verdictdb.core.querying.ola.HyperTableCube;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertable;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sql.syntax.SqlSyntax;

public class AsyncAggScaleExecutionNode extends ProjectionNode {

  HashMap<List<Integer>, Double> scaleFactor = new HashMap<>();

  List<ColumnOp> aggColumnlist = new ArrayList<>();

  // Key is the index of scramble table in Dimension, value is the tier column name
  // Only record tables have multiple tiers
  List<Pair<Integer, String>> multipleTierTableTierInfo = new ArrayList<>();



  protected AsyncAggScaleExecutionNode(TempIdCreator namer) {
    super(namer, null);
  }

  public static AsyncAggScaleExecutionNode create(TempIdCreator namer, AggExecutionNode aggNode)
      throws VerdictDBException {
    AsyncAggScaleExecutionNode node = new AsyncAggScaleExecutionNode(namer);

    // Setup select list
    Pair<BaseTable, ExecutionTokenQueue> baseAndQueue = node.createPlaceHolderTable("to_scale_query");
    List<SelectItem> newSelectList = dependent.getSelectQuery().deepcopy().getSelectList();
    if (dependent instanceof AggExecutionNode) {
      for (SelectItem selectItem : newSelectList) {
        // invariant: the agg column must be aliased column
        if (selectItem instanceof AliasedColumn) {
          int index = newSelectList.indexOf(selectItem);
          UnnamedColumn col = ((AliasedColumn) selectItem).getColumn();
          if (AsyncAggScaleExecutionNode.isAggregateColumn(col)) {
            ColumnOp aggColumn = new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                ConstantColumn.valueOf(1.0), new BaseColumn("to_scale_query", ((AliasedColumn) selectItem).getAliasName())
            ));
            node.aggColumnlist.add(aggColumn);
            newSelectList.set(index, new AliasedColumn(aggColumn, ((AliasedColumn) selectItem).getAliasName()));
          } else {
            newSelectList.set(index, new AliasedColumn(new BaseColumn("to_scale_query", ((AliasedColumn) selectItem).getAliasName()),
                ((AliasedColumn) selectItem).getAliasName()));
          }
        }
      }
    }
    else if (dependent instanceof AggCombinerExecutionNode) {
      for (SelectItem selectItem : newSelectList) {
        if (selectItem instanceof AliasedColumn) {
          int index = newSelectList.indexOf(selectItem);
          if (((AliasedColumn) selectItem).getAliasName().matches("s[0-9]+")||((AliasedColumn) selectItem).getAliasName().matches("c[0-9]+")) {
            ColumnOp aggColumn = new ColumnOp("multiply", Arrays.<UnnamedColumn>asList(
                ConstantColumn.valueOf(1.0), new BaseColumn("to_scale_query", ((AliasedColumn) selectItem).getAliasName())
            ));
            node.aggColumnlist.add(aggColumn);
            newSelectList.set(index, new AliasedColumn(aggColumn, ((AliasedColumn) selectItem).getAliasName()));
          } else {
            newSelectList.set(index, new AliasedColumn(new BaseColumn("to_scale_query", ((AliasedColumn) selectItem).getAliasName()),
                ((AliasedColumn) selectItem).getAliasName()));
          }
        }
      }
    }
    // Setup from table

    SelectQuery query = SelectQuery.create(newSelectList, baseAndQueue.getLeft());
    node.setSelectQuery(query);

    // Set this node to broadcast to the parents of asyncNode
    // Also remove the dependency
    for (QueryExecutionNode parent:dependent.getParents()) {
      int index = parent.dependents.indexOf(dependent);
      ExecutionTokenQueue queue = new ExecutionTokenQueue();
      // If parent is AsyncAggExecution, all dependents share a listening queue
      if (parent instanceof AsyncAggExecutionNode) {
        node.addBroadcastingQueue(parent.getListeningQueue(0));
      }
      else {
        parent.getListeningQueues().set(index, queue);
        node.addBroadcastingQueue(queue);
      }
      parent.dependents.set(index, node);
      node.addParent(parent);
    }

    // Set the asyncNode only to broadcast to this node
    // Also set parent
    dependent.getBroadcastingQueues().clear();
    dependent.addBroadcastingQueue(baseAndQueue.getRight());
    dependent.getParents().clear();
    node.addDependency(dependent);

    return node;
  }

  // Currently, only need to judge whether it is sum or count
  // Also replace alias name
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

  @Override
  public SqlConvertable createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    for (ExecutionInfoToken token : tokens) {
      List<HyperTableCube> cubes = (List<HyperTableCube>) token.getValue("hyperTableCube");
      if (cubes != null) {
        // Calculate the scale factor
        scaleFactor = calculateScaleFactor(cubes);
        // Substitute the scale factor
        for (ColumnOp col : aggColumnlist) {
          col.setOperand(0, ConstantColumn.valueOf(scaleFactor));
        }
      }
    }
    return super.createQuery(tokens);
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    return super.createToken(result);
  }

//  @Override
//  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults)
//      throws VerdictDBException {
//
//    for (ExecutionInfoToken downstreamResult : downstreamResults) {
//      List<HyperTableCube> cubes = (List<HyperTableCube>) downstreamResult.getValue("hyperTableCube");
//      if (cubes != null) {
//        // Calculate the scale factor
//        scaleFactor = calculateScaleFactor(cubes);
//        // Substitute the scale factor
//        for (ColumnOp col : aggColumnlist) {
//          col.setOperand(0, ConstantColumn.valueOf(scaleFactor));
//        }
//      }
//    }
//    ExecutionInfoToken token = super.executeNode(conn, downstreamResults);
//    return token;
//  }

  @Override
  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults)
      throws VerdictDBException {
    for (ExecutionInfoToken downstreamResult:downstreamResults) {
      List<HyperTableCube> cubes = (List<HyperTableCube>) downstreamResult.getValue("hyperTableCube");
      if (cubes != null) {
        // Calculate the scale factor
        scaleFactor = calculateScaleFactor(cubes);
        // no multiple tier
        if (scaleFactor.size()==1) {
          // Substitute the scale factor
          Double s = (Double) (scaleFactor.values().toArray())[0];
          for (ColumnOp col : aggColumnlist) {
            col.setOperand(0, ConstantColumn.valueOf(s));
          }
        }
        else {
          String schemaName = (String)downstreamResult.getValue("schemaName");
          String tableName = (String)downstreamResult.getValue("tableName");
          // Need to change the subquery table
          conn.executeUpdate(String.format("alter table %s%s%s.%s%s%s add %sscale_factor%s float", conn.getSyntax().getQuoteString(),
              schemaName, conn.getSyntax().getQuoteString(), conn.getSyntax().getQuoteString(), tableName, conn.getSyntax().getQuoteString(),
              conn.getSyntax().getQuoteString(), conn.getSyntax().getQuoteString()));
          // Update the scale factor column
          for (Map.Entry<List<Integer>, Double> entry:scaleFactor.entrySet()) {
            String condition = setupCondition(entry.getKey(), conn.getSyntax());
            conn.executeUpdate(String.format("update %s%s%s.%s%s%s set %sscale_factor%s=%f where %s", conn.getSyntax().getQuoteString(),
                schemaName, conn.getSyntax().getQuoteString(), conn.getSyntax().getQuoteString(),
                tableName, conn.getSyntax().getQuoteString(), conn.getSyntax().getQuoteString(), conn.getSyntax().getQuoteString(),
                entry.getValue(), condition));
          }
          // let the created column to be the scale factor
          for (ColumnOp col : aggColumnlist) {
            col.setOperand(0, new BaseColumn("to_scale_query", "scale_factor"));
          }
        }
      }
    }
    ExecutionInfoToken token = super.executeNode(conn, downstreamResults);
    return token;
  }

  @Override
  public BaseQueryNode deepcopy() {
    AsyncAggScaleExecutionNode node = new AsyncAggScaleExecutionNode(namer);
    copyFields(this, node);
    node.scaleFactor = scaleFactor;
    node.aggColumnlist = aggColumnlist;
    return node;
  }

  // Currently, assume block size is uniform
  public HashMap<List<Integer>, Double> calculateScaleFactor(List<HyperTableCube> cubes) {
    AsyncAggExecutionNode asyncNode;
    if (this.getParents().size()==2) {
      asyncNode = (AsyncAggExecutionNode)this.getParents().get(1);
    }
    else {
      asyncNode =  (AsyncAggExecutionNode)this.getParents().get(0);
    }
    ScrambleMeta scrambleMeta = asyncNode.getScrambleMeta();
    List<ScrambleMetaForTable> metaForTablesList = new ArrayList<>();
    List<Integer> blockCountList = new ArrayList<>();
    List<Pair<Integer, Integer>> scrambleTableTierInfo = new ArrayList<>();
    for (Dimension d:cubes.get(0).getDimensions()) {
      blockCountList.add(scrambleMeta.getAggregationBlockCount(d.getSchemaName(), d.getTableName()));
      metaForTablesList.add(scrambleMeta.getMetaForTable(d.getSchemaName(), d.getTableName()));
      scrambleTableTierInfo.add(new ImmutablePair<>(cubes.get(0).getDimensions().indexOf(d),
          scrambleMeta.getMetaForTable(d.getSchemaName(), d.getTableName()).getNumberOfTiers()));
      if (scrambleMeta.getMetaForTable(d.getSchemaName(), d.getTableName()).getNumberOfTiers()>1) {
        multipleTierTableTierInfo.add(new ImmutablePair<>(cubes.get(0).getDimensions().indexOf(d),
            scrambleMeta.getMetaForTable(d.getSchemaName(), d.getTableName()).getTierColumn()));
      }
    }

    List<List<Integer>> tierPermuation = generateTierPermuation(scrambleTableTierInfo);
    HashMap<List<Integer>, Double> scaleFactor = new HashMap<>();

    for (HyperTableCube cube:cubes) {
      for (List<Integer> tierlist:tierPermuation) {
        double scale = 1;
        for (int i=0;i<tierlist.size();i++) {
          int tier = tierlist.get(i);
          Dimension d = cube.getDimensions().get(i);
          int blockIndex = d.getEnd();
          scale = scale * metaForTablesList.get(i).getCumulativeProbabilityDistribution(tier).get(blockIndex);
        }
        scaleFactor.put(tierlist, 1/scale);
      }
    }
    return scaleFactor;

  }

  List<List<Integer>> generateTierPermuation(List<Pair<Integer, Integer>> scrambleTableTierInfo) {
    if (scrambleTableTierInfo.size()==1) {
      List<List<Integer>> res = new ArrayList<>();
      for (int tier=0;tier<scrambleTableTierInfo.get(0).getRight();tier++) {
        res.add(Arrays.asList(tier));
      }
      return res;
    }
    else {
      List<Pair<Integer, Integer>> next = scrambleTableTierInfo.subList(1, scrambleTableTierInfo.size());
      List<List<Integer>> subres = generateTierPermuation(next);
      List<List<Integer>> res = new ArrayList<>();
      for (int tier=0;tier<scrambleTableTierInfo.get(0).getRight();tier++) {
        for (List<Integer> tierlist:subres) {
          List<Integer> newTierlist = tierlist;
          newTierlist.add(0, tier);
          res.add(newTierlist);
        }
      }
      return res;
    }
  }

  String setupCondition(List<Integer> tierlist, SqlSyntax syntax) {
    String condition = "";
    for (int i=0;i<multipleTierTableTierInfo.size();i++) {
      condition = condition + syntax.getQuoteString() + multipleTierTableTierInfo.get(i).getRight() + syntax.getQuoteString()
          + "=" + tierlist.get(multipleTierTableTierInfo.get(i).getLeft());
      if (i!=multipleTierTableTierInfo.size()-1) {
        condition = condition + " and ";
      }
    }
    return condition;
  }
}
