package org.verdictdb.core.sql;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.query.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

public class AliasGen {

  private MetaData meta;

  private static long itemID = 1;

  //key is the column name and value is table alias name
  private HashMap<String, String> colNameAndTableAlias = new HashMap<>();

  //key is the schema name and table name and the value is table alias name
  private HashMap<Pair<String, String>, String> tableInfoAndAlias = new HashMap<>();

  //key is the select column name, value is their alias
  private HashMap<String, String> colNameAndColAlias = new HashMap<>();

  public AliasGen(MetaData meta){
    this.meta = meta;
  }

  private BaseColumn replaceBaseColumn(BaseColumn col){
    if (col.getTableSourceAlias().equals("")){
      col.setTableSourceAlias(colNameAndTableAlias.get(col.getColumnName()));
    }
    return col;
  }

  private List<SelectItem> replaceSelectList(List<SelectItem> selectItemList){
    List<SelectItem> newSelectItemList = new ArrayList<>();
    for (SelectItem sel:selectItemList){
      if (!(sel instanceof AliasedColumn) && !(sel instanceof AsteriskColumn)){
        if (sel instanceof BaseColumn){
          sel = replaceBaseColumn((BaseColumn) sel);
          colNameAndColAlias.put(((BaseColumn) sel).getColumnName(), "vc"+itemID);
          newSelectItemList.add(new AliasedColumn((BaseColumn)sel, "vc"+itemID++));
        }
        else if (sel instanceof ColumnOp){
          //First replace the possible base column inside the columnop using the same way we did on Where clause
          sel = replaceFilter((ColumnOp)sel);

          if (((ColumnOp) sel).getOpType().equals("count")){
            newSelectItemList.add(new AliasedColumn((ColumnOp)sel, "c"+itemID++));
          }
          else if (((ColumnOp) sel).getOpType().equals("sum")){
            newSelectItemList.add(new AliasedColumn((ColumnOp)sel, "s"+itemID++));
          }
          else if (((ColumnOp) sel).getOpType().equals("avg")){
            newSelectItemList.add(new AliasedColumn((ColumnOp)sel, "a"+itemID++));
          }
          else if (((ColumnOp) sel).getOpType().equals("countdistinct")){
            newSelectItemList.add(new AliasedColumn((ColumnOp)sel, "cd"+itemID++));
          }
          else {
            newSelectItemList.add(new AliasedColumn((ColumnOp)sel, "vc"+itemID++));
          }
        }
      }
      else {
        if (sel instanceof AliasedColumn){
          ((AliasedColumn) sel).setColumn(replaceFilter(((AliasedColumn) sel).getColumn()));
        }
        newSelectItemList.add(sel);
        if (sel instanceof AliasedColumn && ((AliasedColumn) sel).getColumn() instanceof BaseColumn){
          colNameAndColAlias.put(((BaseColumn) ((AliasedColumn) sel).getColumn()).getColumnName(),
              ((AliasedColumn) sel).getAliasName());
        }
      }
    }
    return newSelectItemList;
  }

  //Use BFS to search all the condition.
  private UnnamedColumn replaceFilter(UnnamedColumn condition) {
    List<UnnamedColumn> searchList = new Vector<>();
    searchList.add(condition);
    while (!searchList.isEmpty()){
      UnnamedColumn cond = searchList.get(0);
      searchList.remove(0);
      if (cond instanceof BaseColumn){
        cond = replaceBaseColumn((BaseColumn) cond);
      }
      else if (cond instanceof ColumnOp){
        for (UnnamedColumn col:((ColumnOp) cond).getOperands()){
          searchList.add(col);
        }
      }
      else if (cond instanceof SubqueryColumn){
        AliasGen g = new AliasGen(meta);
        g.setColNameAndColAlias(colNameAndColAlias);
        g.setColNameAndTableAlias(colNameAndTableAlias);
        g.setTableInfoAndAlias(tableInfoAndAlias);
        SelectQueryOp newSubquery = g.replaceRelationAlias(((SubqueryColumn) cond).getSubquery());
        ((SubqueryColumn) cond).setSubquery(newSubquery);
      }
    }
    return condition;
  }

  private List<GroupingAttribute> replaceGroupby(List<GroupingAttribute> groupingAttributeList){
    List<GroupingAttribute> newGroupby = new ArrayList<>();
    for (GroupingAttribute g:groupingAttributeList){
      if (colNameAndColAlias.containsKey(((AliasReference)g).getAliasName())){
        newGroupby.add(new AliasReference(colNameAndColAlias.get(((AliasReference)g).getAliasName())));
      }
      else newGroupby.add(g);
    }
    return newGroupby;
  }

  private List<OrderbyAttribute> replaceOrderby(List<OrderbyAttribute> orderbyAttributesList){
    List<OrderbyAttribute> newOrderby = new ArrayList<>();
    for (OrderbyAttribute o:orderbyAttributesList){
      if (colNameAndColAlias.containsKey(o.getAttributeName())){
        newOrderby.add(new OrderbyAttribute(colNameAndColAlias.get(o.getAttributeName()), o.getOrder()));
      }
      else newOrderby.add(o);
    }
    return newOrderby;
  }

  /*
   * return the ColName contained by the table
   */
  private Pair<List<String>, AbstractRelation> setupTableSource(AbstractRelation table){
    if (!table.getAliasName().isPresent() && !(table instanceof JoinTable)){
      table.setAliasName("vt"+itemID++);
    }
    if (table instanceof BaseTable){
      List<String> colName = new ArrayList<>();
      if (((BaseTable) table).getSchemaName()==null){
        ((BaseTable) table).setSchemaName(meta.getDefaultSchema());
      }
      HashMap<MetaData.tableInfo, List<ImmutablePair<String, MetaData.dataType>>> tablesData = meta.getTablesData();
      List<ImmutablePair<String, MetaData.dataType>> cols = tablesData.get(MetaData.tableInfo.getTableInfo(
          ((BaseTable) table).getSchemaName(), ((BaseTable) table).getTableName()));
      for (Pair<String, MetaData.dataType> c:cols){
        colNameAndTableAlias.put(c.getKey(), table.getAliasName().get());
        colName.add(c.getKey());
      }
      tableInfoAndAlias.put(new ImmutablePair<>(((BaseTable) table).getSchemaName(), ((BaseTable) table).getTableName()),
          table.getAliasName().get());
      return new ImmutablePair<>(colName, table);
    }
    else if (table instanceof JoinTable) {
      List<String> joinColName = new ArrayList<>();
      for (int i=0;i<((JoinTable) table).getJoinList().size();i++) {
        joinColName.addAll(setupTableSource(((JoinTable) table).getJoinList().get(i)).getKey());
        if (i!=0) {
          ((JoinTable) table).getCondition().set(i-1, replaceFilter(((JoinTable) table).getCondition().get(i-1)));
        }
      }
      return new ImmutablePair<>(joinColName, table);
    }
    else if (table instanceof SelectQueryOp) {
      List<String> colName = new ArrayList<>();
      AliasGen g = new AliasGen(meta);
      g.setTableInfoAndAlias(tableInfoAndAlias);
      g.setColNameAndTableAlias(colNameAndTableAlias);
      g.setColNameAndColAlias(colNameAndColAlias);
      String aliasName = table.getAliasName().get();
      table = g.replaceRelationAlias((SelectQueryOp) table);
      table.setAliasName(aliasName);
      // Invariant: Only Aliased Column or Asterisk Column should appear in the subquery
      for (SelectItem sel:((SelectQueryOp) table).getSelectList()){
        if (sel instanceof AliasedColumn){
          //If the aliased name of the column is replaced by ourselves, we should remember the column name
          if (((AliasedColumn) sel).getColumn() instanceof BaseColumn && ((AliasedColumn) sel).getAliasName().matches("^vc[0-9]+$")){
            colNameAndTableAlias.put(((BaseColumn) ((AliasedColumn) sel).getColumn()).getColumnName(),
                table.getAliasName().get());
          }
          else colNameAndTableAlias.put(((AliasedColumn) sel).getAliasName(), table.getAliasName().get());
          colName.add(((AliasedColumn) sel).getAliasName());
        }
        else if (sel instanceof AsteriskColumn){
          //put all the columns in the fromlist of subquery to the colNameAndTableAlias
          HashMap<String, String> subqueryColumnList = g.getColNameAndTableAlias();
          for (String col:subqueryColumnList.keySet()){
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
  public List<AbstractRelation> setupTableSources(SelectQueryOp relationToAlias) {
    List<AbstractRelation> fromList = relationToAlias.getFromList();
    for (int i=0;i<fromList.size();i++) {
      fromList.set(i, setupTableSource(fromList.get(i)).getValue());
    }
    return fromList;
  }

  public SelectQueryOp replaceRelationAlias(SelectQueryOp relationToAlias){
    List<AbstractRelation> fromList = setupTableSources(relationToAlias);
    List<SelectItem> selectItemList = replaceSelectList(relationToAlias.getSelectList());
    SelectQueryOp AliasedRelation = SelectQueryOp.getSelectQueryOp(selectItemList, fromList);

    //Filter
    UnnamedColumn where;
    if (relationToAlias.getFilter().isPresent()){
      where = replaceFilter(relationToAlias.getFilter().get());
      AliasedRelation.addFilterByAnd(where);
    }

    //Group by
    List<GroupingAttribute> groupby;
    if (relationToAlias.getGroupby().size()!=0){
      groupby = replaceGroupby(relationToAlias.getGroupby());
      AliasedRelation.addGroupby(groupby);
    }

    //Having
    UnnamedColumn having;
    if (relationToAlias.getHaving().isPresent()){
      having = replaceFilter(relationToAlias.getHaving().get());
      AliasedRelation.addHavingByAnd(having);
    }

    //Order by
    List<OrderbyAttribute> orderby;
    if (relationToAlias.getOrderby().size()!=0){
      orderby = replaceOrderby(relationToAlias.getOrderby());
      AliasedRelation.addOrderby(orderby);
    }

    if (relationToAlias.getLimit().isPresent()){
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

  public void setColNameAndTableAlias(HashMap<String, String> colNameAndTableAlias){
    for (String key:colNameAndTableAlias.keySet()){
      this.colNameAndTableAlias.put(key, colNameAndTableAlias.get(key));
    }
  }

  public void setTableInfoAndAlias(HashMap<Pair<String, String>, String> tableInfoAndAlias){
    for (Pair<String, String> key:tableInfoAndAlias.keySet()){
      this.tableInfoAndAlias.put(key, tableInfoAndAlias.get(key));
    }
  }

  public void setColNameAndColAlias(HashMap<String, String> colNameAndColAlias) {
    for (String key:colNameAndColAlias.keySet()){
      this.colNameAndColAlias.put(key, colNameAndColAlias.get(key));
    }
  }

  public static void resetItemID(){
    itemID = 1;
  }
}
