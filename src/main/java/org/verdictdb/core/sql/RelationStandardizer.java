package org.verdictdb.core.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.MetaDataProvider;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.AliasReference;
import org.verdictdb.core.query.AliasedColumn;
import org.verdictdb.core.query.AsteriskColumn;
import org.verdictdb.core.query.BaseColumn;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.GroupingAttribute;
import org.verdictdb.core.query.JoinTable;
import org.verdictdb.core.query.OrderbyAttribute;
import org.verdictdb.core.query.SelectItem;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.query.SubqueryColumn;
import org.verdictdb.core.query.UnnamedColumn;
import org.verdictdb.exception.VerdictDBDbmsException;

public class RelationStandardizer {

  private MetaDataProvider meta;

  private static long itemID = 1;

  //key is the column name and value is table alias name
  private HashMap<String, String> colNameAndTableAlias = new HashMap<>();

  //key is the schema name and table name and the value is table alias name
  private HashMap<Pair<String, String>, String> tableInfoAndAlias = new HashMap<>();

  //key is the select column name, value is their alias
  private HashMap<String, String> colNameAndColAlias = new HashMap<>();

  /***
   * If From list is a subquery, we need to record the column alias name in colNameAndTempColAlias
   * so that we can replace the select item with the column alias name we generate.
   */
  private HashMap<String, String> colNameAndTempColAlias = new HashMap<>();


  public RelationStandardizer(MetaDataProvider meta) {
    this.meta = meta;
  }

  private BaseColumn replaceBaseColumn(BaseColumn col) {
    if (col.getTableSourceAlias().equals("")) {
      if (!(col.getSchemaName().equals(""))) {
        col.setTableSourceAlias(tableInfoAndAlias.get(
            new ImmutablePair<>(col.getSchemaName(), col.getTableName())));
      } else {
        col.setTableSourceAlias(colNameAndTableAlias.get(col.getColumnName()));
      }
    }
    if (colNameAndTempColAlias.containsKey(col.getColumnName())) {
      col.setColumnName(colNameAndTempColAlias.get(col.getColumnName()));
    }
    return col;
  }

  private List<SelectItem> replaceSelectList(List<SelectItem> selectItemList) throws VerdictDBDbmsException {
    List<SelectItem> newSelectItemList = new ArrayList<>();
    for (SelectItem sel : selectItemList) {
      if (!(sel instanceof AliasedColumn) && !(sel instanceof AsteriskColumn)) {
        if (sel instanceof BaseColumn) {
          sel = replaceBaseColumn((BaseColumn) sel);
          colNameAndColAlias.put(((BaseColumn) sel).getColumnName(), "vc" + itemID);
          newSelectItemList.add(new AliasedColumn((BaseColumn) sel, "vc" + itemID++));
        } else if (sel instanceof ColumnOp) {
          //First replace the possible base column inside the columnop using the same way we did on Where clause
          sel = replaceFilter((ColumnOp) sel);

          if (((ColumnOp) sel).getOpType().equals("count")) {
            newSelectItemList.add(new AliasedColumn((ColumnOp) sel, "c" + itemID++));
          } else if (((ColumnOp) sel).getOpType().equals("sum")) {
            newSelectItemList.add(new AliasedColumn((ColumnOp) sel, "s" + itemID++));
          } else if (((ColumnOp) sel).getOpType().equals("avg")) {
            newSelectItemList.add(new AliasedColumn((ColumnOp) sel, "a" + itemID++));
          } else if (((ColumnOp) sel).getOpType().equals("countdistinct")) {
            newSelectItemList.add(new AliasedColumn((ColumnOp) sel, "cd" + itemID++));
          } else {
            newSelectItemList.add(new AliasedColumn((ColumnOp) sel, "vc" + itemID++));
          }
        }
      } else {
        if (sel instanceof AliasedColumn) {
          ((AliasedColumn) sel).setColumn(replaceFilter(((AliasedColumn) sel).getColumn()));
        }
        newSelectItemList.add(sel);
        if (sel instanceof AliasedColumn && ((AliasedColumn) sel).getColumn() instanceof BaseColumn) {
          colNameAndColAlias.put(((BaseColumn) ((AliasedColumn) sel).getColumn()).getColumnName(),
              ((AliasedColumn) sel).getAliasName());
        }
      }
    }
    return newSelectItemList;
  }

  //Use BFS to search all the condition.
  private UnnamedColumn replaceFilter(UnnamedColumn condition) throws VerdictDBDbmsException {
    List<UnnamedColumn> searchList = new Vector<>();
    searchList.add(condition);
    while (!searchList.isEmpty()) {
      UnnamedColumn cond = searchList.get(0);
      searchList.remove(0);
      if (cond instanceof BaseColumn) {
        cond = replaceBaseColumn((BaseColumn) cond);
      } else if (cond instanceof ColumnOp) {
        for (UnnamedColumn col : ((ColumnOp) cond).getOperands()) {
          searchList.add(col);
        }
      } else if (cond instanceof SubqueryColumn) {
        RelationStandardizer g = new RelationStandardizer(meta);
        g.setColNameAndColAlias(colNameAndColAlias);
        g.setColNameAndTableAlias(colNameAndTableAlias);
        g.setTableInfoAndAlias(tableInfoAndAlias);
        SelectQuery newSubquery = g.standardize(((SubqueryColumn) cond).getSubquery());
        ((SubqueryColumn) cond).setSubquery(newSubquery);
      }
    }
    return condition;
  }

  private List<GroupingAttribute> replaceGroupby(List<GroupingAttribute> groupingAttributeList) {
    List<GroupingAttribute> newGroupby = new ArrayList<>();
    for (GroupingAttribute g : groupingAttributeList) {
      if (colNameAndColAlias.containsKey(((AliasReference) g).getAliasName())) {
        newGroupby.add(new AliasReference(colNameAndColAlias.get(((AliasReference) g).getAliasName())));
      } else newGroupby.add(g);
    }
    return newGroupby;
  }

  private List<OrderbyAttribute> replaceOrderby(List<OrderbyAttribute> orderbyAttributesList) {
    List<OrderbyAttribute> newOrderby = new ArrayList<>();
    for (OrderbyAttribute o : orderbyAttributesList) {
      if (colNameAndColAlias.containsKey(o.getAttributeName())) {
        newOrderby.add(new OrderbyAttribute(colNameAndColAlias.get(o.getAttributeName()), o.getOrder()));
      } else newOrderby.add(o);
    }
    return newOrderby;
  }

  /*
   * return the ColName contained by the table
   */
  private Pair<List<String>, AbstractRelation> setupTableSource(AbstractRelation table) throws VerdictDBDbmsException {
    if (!table.getAliasName().isPresent() && !(table instanceof JoinTable)) {
      table.setAliasName("vt" + itemID++);
    }
    if (table instanceof BaseTable) {
      List<String> colName = new ArrayList<>();
      if (((BaseTable) table).getSchemaName() == null) {
        ((BaseTable) table).setSchemaName(meta.getDefaultSchema());
      }
      List<Pair<String, Integer>> cols = meta.getColumns(((BaseTable) table).getSchemaName(), ((BaseTable) table).getTableName());
      for (Pair<String, Integer> c : cols) {
        colNameAndTableAlias.put(c.getKey(), table.getAliasName().get());
        colName.add(c.getKey());
      }
      tableInfoAndAlias.put(new ImmutablePair<>(((BaseTable) table).getSchemaName(), ((BaseTable) table).getTableName()),
          table.getAliasName().get());
      return new ImmutablePair<>(colName, table);
    } else if (table instanceof JoinTable) {
      List<String> joinColName = new ArrayList<>();
      for (int i = 0; i < ((JoinTable) table).getJoinList().size(); i++) {
        Pair<List<String>, AbstractRelation> result = setupTableSource(((JoinTable) table).getJoinList().get(i));
        ((JoinTable) table).getJoinList().set(i, result.getValue());
        joinColName.addAll(result.getKey());
        if (i != 0) {
          ((JoinTable) table).getCondition().set(i - 1, replaceFilter(((JoinTable) table).getCondition().get(i - 1)));
        }
      }
      return new ImmutablePair<>(joinColName, table);
    } else if (table instanceof SelectQuery) {
      List<String> colName = new ArrayList<>();
      RelationStandardizer g = new RelationStandardizer(meta);
      g.setTableInfoAndAlias(tableInfoAndAlias);
      g.setColNameAndTableAlias(colNameAndTableAlias);
      g.setColNameAndColAlias(colNameAndColAlias);
      String aliasName = table.getAliasName().get();
      table = g.standardize((SelectQuery) table);
      table.setAliasName(aliasName);
      // Invariant: Only Aliased Column or Asterisk Column should appear in the subquery
      for (SelectItem sel : ((SelectQuery) table).getSelectList()) {
        if (sel instanceof AliasedColumn) {
          //If the aliased name of the column is replaced by ourselves, we should remember the column name
          if (((AliasedColumn) sel).getColumn() instanceof BaseColumn && ((AliasedColumn) sel).getAliasName().matches("^vc[0-9]+$")) {
            colNameAndTableAlias.put(((BaseColumn) ((AliasedColumn) sel).getColumn()).getColumnName(),
                table.getAliasName().get());
            colNameAndTempColAlias.put(((BaseColumn) ((AliasedColumn) sel).getColumn()).getColumnName(), ((AliasedColumn) sel).getAliasName());
          }
          else colNameAndTableAlias.put(((AliasedColumn) sel).getAliasName(), table.getAliasName().get());
          colName.add(((AliasedColumn) sel).getAliasName());
        } else if (sel instanceof AsteriskColumn) {
          //put all the columns in the fromlist of subquery to the colNameAndTableAlias
          HashMap<String, String> subqueryColumnList = g.getColNameAndTableAlias();
          for (String col : subqueryColumnList.keySet()) {
            colNameAndTableAlias.put(col, table.getAliasName().get());
            colName.add(col);
          }
        }
      }
      return new ImmutablePair<>(colName, table);
    }
    return null;
  }

  /*
   * Figure out the table alias and the columns the table have
   */
  private List<AbstractRelation> setupTableSources(SelectQuery relationToAlias) throws VerdictDBDbmsException {
    List<AbstractRelation> fromList = relationToAlias.getFromList();
    for (int i = 0; i < fromList.size(); i++) {
      fromList.set(i, setupTableSource(fromList.get(i)).getValue());
    }
    return fromList;
  }

  public SelectQuery standardize(SelectQuery relationToAlias) throws VerdictDBDbmsException {
    List<AbstractRelation> fromList = setupTableSources(relationToAlias);
    List<SelectItem> selectItemList = replaceSelectList(relationToAlias.getSelectList());
    SelectQuery AliasedRelation = SelectQuery.create(selectItemList, fromList);

    //Filter
    UnnamedColumn where;
    if (relationToAlias.getFilter().isPresent()) {
      where = replaceFilter(relationToAlias.getFilter().get());
      AliasedRelation.addFilterByAnd(where);
    }

    //Group by
    List<GroupingAttribute> groupby;
    if (relationToAlias.getGroupby().size() != 0) {
      groupby = replaceGroupby(relationToAlias.getGroupby());
      AliasedRelation.addGroupby(groupby);
    }

    //Having
    UnnamedColumn having;
    if (relationToAlias.getHaving().isPresent()) {
      having = replaceFilter(relationToAlias.getHaving().get());
      AliasedRelation.addHavingByAnd(having);
    }

    //Order by
    List<OrderbyAttribute> orderby;
    if (relationToAlias.getOrderby().size() != 0) {
      orderby = replaceOrderby(relationToAlias.getOrderby());
      AliasedRelation.addOrderby(orderby);
    }

    if (relationToAlias.getLimit().isPresent()) {
      AliasedRelation.addLimit(relationToAlias.getLimit().get());
    }
    return AliasedRelation;
  }

  public HashMap<String, String> getColNameAndColAlias() {
    return colNameAndColAlias;
  }

  public HashMap<Pair<String, String>, String> getTableInfoAndAlias() {
    return tableInfoAndAlias;
  }

  public HashMap<String, String> getColNameAndTableAlias() {
    return colNameAndTableAlias;
  }

  public void setColNameAndTableAlias(HashMap<String, String> colNameAndTableAlias) {
    for (String key : colNameAndTableAlias.keySet()) {
      this.colNameAndTableAlias.put(key, colNameAndTableAlias.get(key));
    }
  }

  public void setTableInfoAndAlias(HashMap<Pair<String, String>, String> tableInfoAndAlias) {
    for (Pair<String, String> key : tableInfoAndAlias.keySet()) {
      this.tableInfoAndAlias.put(key, tableInfoAndAlias.get(key));
    }
  }

  public void setColNameAndColAlias(HashMap<String, String> colNameAndColAlias) {
    for (String key : colNameAndColAlias.keySet()) {
      this.colNameAndColAlias.put(key, colNameAndColAlias.get(key));
    }
  }

  public static void resetItemID() {
    itemID = 1;
  }

}
