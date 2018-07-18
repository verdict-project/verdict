package org.verdictdb.sqlreader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.MetaDataProvider;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasReference;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.GroupingAttribute;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.OrderbyAttribute;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBDbmsException;

public class RelationStandardizer {

  private MetaDataProvider meta;

  private static long itemID = 1;

  private static long duplicateIdentifer = 1;

  private static String verdictTableAliasPrefix = "vt";

  //key is the column name and value is table alias name
  private HashMap<String, String> colNameAndTableAlias = new HashMap<>();

  //key is the schema name and table name and the value is table alias name
  private HashMap<Pair<String, String>, String> tableInfoAndAlias = new HashMap<>();

  //key is the select column name, value is their alias
  private HashMap<String, String> colNameAndColAlias = new HashMap<>();

  //key is columnOp, value is their alias name
  private HashMap<ColumnOp, String> columnOpAliasMap = new HashMap<>();

  //key is schema name, column name, value is alias
  //only store value if there are duplicate column names
  private HashMap<Pair<String, String>, String> duplicateColNameAndColAlias = new HashMap<>();
  /***
   * If From list is a subquery, we need to record the column alias name in colNameAndTempColAlias
   * so that we can replace the select item with the column alias name we generate.
   */
  private HashMap<String, String> colNameAndTempColAlias = new HashMap<>();


  // Since we replace all table alias using our generated alias name, this map will record the table alias name
  // we replaced.
  private HashMap<String, String> oldTableAliasMap = new HashMap<>();


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
    if (oldTableAliasMap.containsKey(col.getTableSourceAlias())) {
      col.setTableSourceAlias(oldTableAliasMap.get(col.getTableSourceAlias()));
    }
    if (tableInfoAndAlias.containsValue(col.getTableSourceAlias())) {
      for (Map.Entry<Pair<String, String>, String> entry:tableInfoAndAlias.entrySet()) {
        if (entry.getValue().equals(col.getTableSourceAlias())) {
          col.setSchemaName(entry.getKey().getLeft());
          col.setTableName(entry.getKey().getRight());
          break;
        }
      }
    }
    if (col.getSchemaName().equals("")) {
      col.setSchemaName(meta.getDefaultSchema());
      if (tableInfoAndAlias.containsKey(new ImmutablePair<>(col.getSchemaName(), col.getTableSourceAlias()))) {
        col.setTableSourceAlias(tableInfoAndAlias.get(new ImmutablePair<>(col.getSchemaName(), col.getTableSourceAlias())));
      }
    }


    return col;
  }

  private List<SelectItem> replaceSelectList(List<SelectItem> selectItemList) throws VerdictDBDbmsException {
    List<SelectItem> newSelectItemList = new ArrayList<>();
    for (SelectItem sel : selectItemList) {
      if (!(sel instanceof AliasedColumn) && !(sel instanceof AsteriskColumn)) {
        if (sel instanceof BaseColumn) {
          sel = replaceBaseColumn((BaseColumn) sel);
          if (!colNameAndColAlias.containsValue(((BaseColumn) sel).getColumnName())) {
            colNameAndColAlias.put(((BaseColumn) sel).getColumnName(), ((BaseColumn) sel).getColumnName());
            newSelectItemList.add(new AliasedColumn((BaseColumn) sel, ((BaseColumn) sel).getColumnName()));
          } else {
            duplicateColNameAndColAlias.put(new ImmutablePair<>(((BaseColumn) sel).getTableSourceAlias(), ((BaseColumn) sel).getColumnName()),
                ((BaseColumn) sel).getColumnName()+duplicateIdentifer);
            newSelectItemList.add(new AliasedColumn((BaseColumn) sel, ((BaseColumn) sel).getColumnName()+duplicateIdentifer++));
          }
        } else if (sel instanceof ColumnOp) {
          //First replace the possible base column inside the columnop using the same way we did on Where clause
          sel = replaceFilter((ColumnOp) sel);

          if (((ColumnOp) sel).getOpType().equals("count")) {
            columnOpAliasMap.put((ColumnOp)sel, "c" + itemID);
            newSelectItemList.add(new AliasedColumn((ColumnOp) sel, "c" + itemID++));
          } else if (((ColumnOp) sel).getOpType().equals("sum")) {
            columnOpAliasMap.put((ColumnOp)sel, "s" + itemID);
            newSelectItemList.add(new AliasedColumn((ColumnOp) sel, "s" + itemID++));
          } else if (((ColumnOp) sel).getOpType().equals("avg")) {
            columnOpAliasMap.put((ColumnOp)sel, "a" + itemID);
            newSelectItemList.add(new AliasedColumn((ColumnOp) sel, "a" + itemID++));
          } else if (((ColumnOp) sel).getOpType().equals("countdistinct")) {
            columnOpAliasMap.put((ColumnOp)sel, "cd" + itemID);
            newSelectItemList.add(new AliasedColumn((ColumnOp) sel, "cd" + itemID++));
          } else {
            columnOpAliasMap.put((ColumnOp)sel, "vc" + itemID);
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
        } else if (sel instanceof AliasedColumn && ((AliasedColumn) sel).getColumn() instanceof ColumnOp) {
          columnOpAliasMap.put(((ColumnOp) ((AliasedColumn) sel).getColumn()),
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
        g.oldTableAliasMap.putAll(oldTableAliasMap);
        g.setColNameAndColAlias(colNameAndColAlias);
        g.setColumnOpAliasMap(columnOpAliasMap);
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
      if (g instanceof BaseColumn) {
        if (((BaseColumn) g).getTableSourceAlias() != null) {
          String tableSource = ((BaseColumn) g).getTableSourceAlias();
          String columnName = ((BaseColumn) g).getColumnName();
          if (duplicateColNameAndColAlias.containsKey(new ImmutablePair<>(tableSource, columnName))) {
            newGroupby.add(new AliasReference(tableSource, duplicateColNameAndColAlias.get(new ImmutablePair<>(tableSource, columnName))));
          } else if (colNameAndColAlias.containsKey(columnName)) {
            newGroupby.add(new AliasReference(colNameAndColAlias.get(columnName)));
          } else newGroupby.add(new AliasReference(((BaseColumn) g).getColumnName()));
        } else if (colNameAndColAlias.containsKey(((BaseColumn) g).getColumnName())) {
          newGroupby.add(new AliasReference(colNameAndColAlias.get(((BaseColumn) g).getColumnName())));
        } else newGroupby.add(new AliasReference(((BaseColumn) g).getColumnName()));
      }
      else {
        newGroupby.add(g);
      }
      else if (colNameAndColAlias.containsKey(((AliasReference) g).getAliasName())) {
        newGroupby.add(new AliasReference(colNameAndColAlias.get(((AliasReference) g).getAliasName())));
      }
      else if (((AliasReference) g).getAliasName().equals("") &&
          ((AliasReference) g).getColumn() instanceof ColumnOp && columnOpAliasMap.containsKey((ColumnOp) ((AliasReference) g).getColumn())){
        newGroupby.add(new AliasReference(columnOpAliasMap.get((ColumnOp) ((AliasReference) g).getColumn())));
      }
      else newGroupby.add(g);
    }
    return newGroupby;
  }

  private List<OrderbyAttribute> replaceOrderby(List<OrderbyAttribute> orderbyAttributesList) {
    List<OrderbyAttribute> newOrderby = new ArrayList<>();
    for (OrderbyAttribute o : orderbyAttributesList) {
      if (o.getAliasName().getTableAlias()!=null) {
        String tableSource = o.getAliasName().getTableAlias();
        String alias = o.getAliasName().getAliasName();
        if (duplicateColNameAndColAlias.containsKey(new ImmutablePair<>(tableSource, alias))) {
          newOrderby.add(new OrderbyAttribute(duplicateColNameAndColAlias.get(new ImmutablePair<>(tableSource, alias)), o.getOrder()));
        }
        else if (colNameAndColAlias.containsKey(o.getAttributeName())) {
          newOrderby.add(new OrderbyAttribute(colNameAndColAlias.get(o.getAttributeName()), o.getOrder()));
        } else newOrderby.add(o);
      }
      else if (colNameAndColAlias.containsKey(o.getAttributeName())) {
        newOrderby.add(new OrderbyAttribute(colNameAndColAlias.get(o.getAttributeName()), o.getOrder()));
      }
      else if (colNameAndColAlias.containsKey(o.getAttributeName())) {
        newOrderby.add(new OrderbyAttribute(colNameAndColAlias.get(o.getAttributeName()), o.getOrder()));
      }
      else newOrderby.add(o);
    }
    return newOrderby;
  }

  /*
   * return the ColName contained by the table
   */
  private Pair<List<String>, AbstractRelation> setupTableSource(AbstractRelation table) throws VerdictDBDbmsException {
    // in order to prevent informal table alias, we replace all table alias
    if (!(table instanceof JoinTable)) {
      if (table.getAliasName().isPresent()) {
        String alias = table.getAliasName().get();
        alias = alias.replace("`", "");
        alias = alias.replace("\"", "");
        oldTableAliasMap.put(alias, verdictTableAliasPrefix+itemID);
      }
      table.setAliasName(verdictTableAliasPrefix + itemID++);
    }
    //if (!table.getAliasName().isPresent() && !(table instanceof JoinTable)) {
    //  table.setAliasName(verdictTableAliasPrefix + itemID++);
    //}
    if (table instanceof BaseTable) {
      List<String> colName = new ArrayList<>();
      if (((BaseTable) table).getSchemaName() == null) {
        ((BaseTable) table).setSchemaName(meta.getDefaultSchema());
      }
      List<Pair<String, String>> cols = meta.getColumns(((BaseTable) table).getSchemaName(), ((BaseTable) table).getTableName());
      for (Pair<String, String> c : cols) {
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
      g.oldTableAliasMap.putAll(oldTableAliasMap);
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

  public void setColumnOpAliasMap(HashMap<ColumnOp, String> columnOpAliasMap) {
    this.columnOpAliasMap = columnOpAliasMap;
  }

  public HashMap<ColumnOp, String> getColumnOpAliasMap() {
    return columnOpAliasMap;
  }

  public static void resetItemID() {
    itemID = 1;
  }

}
