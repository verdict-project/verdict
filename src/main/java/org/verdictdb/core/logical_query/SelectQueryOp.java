package org.verdictdb.core.logical_query;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SelectQueryOp implements AbstractRelation, UnnamedColumn {
    
    List<SelectItem> selectList = new ArrayList<>();
    
    List<AbstractRelation> fromList = new ArrayList<>();
    
    Optional<UnnamedColumn> filter = Optional.empty();
    
    List<GroupingAttribute> groupby = new ArrayList<>();

    List<OrderbyAttribute> orderby = new ArrayList<>();

    Optional<UnnamedColumn> having = Optional.empty();

    Optional<UnnamedColumn> limit = Optional.empty();
    
    Optional<String> aliasName = Optional.empty();
    
    public SelectQueryOp() {}
    
    public static SelectQueryOp getSelectQueryOp(List<SelectItem> columns, AbstractRelation relation) {
        SelectQueryOp sel = new SelectQueryOp();
        for (SelectItem c : columns) {
            sel.addSelectItem(c);
        }
        sel.addTableSource(relation);
        return sel;
    }

    public static SelectQueryOp getSelectQueryOp(List<SelectItem> columns, List<AbstractRelation> relation) {
        SelectQueryOp sel = new SelectQueryOp();
        for (SelectItem c : columns) {
            sel.addSelectItem(c);
        }
        for (AbstractRelation r : relation) {
            sel.addTableSource(r);
        }
        return sel;
    }

    public static SelectQueryOp getSelectQueryOp(List<SelectItem> columns, AbstractRelation relation, UnnamedColumn predicate) {
        SelectQueryOp sel = new SelectQueryOp();
        for (SelectItem c : columns) {
            sel.addSelectItem(c);
        }
        sel.addTableSource(relation);
        sel.filter = Optional.of(predicate);
        return sel;
    }
    
    public void addSelectItem(SelectItem column) {
        selectList.add(column);
    }
    
    public void addTableSource(AbstractRelation relation) {
        fromList.add(relation);
    }
    
    public void addFilterByAnd(UnnamedColumn predicate) {
        if (!filter.isPresent()) {
            filter = Optional.of(predicate);
        }
        else {
            filter = Optional.<UnnamedColumn>of(ColumnOp.and(filter.get(), predicate));
        }
    }
    
    public void setAliasName(String aliasName) {
        this.aliasName = Optional.of(aliasName);
    }
    
    public void addGroupby(GroupingAttribute column) {
        groupby.add(column);
    }

    public void addGroupby(List<GroupingAttribute> columns){
        groupby.addAll(columns);
    }

    public void addOrderby(OrderbyAttribute column) {
        orderby.add(column);
    }

    public void addOrderby(List<OrderbyAttribute> columns) {orderby.addAll(columns); }

    public void addHavingByAnd(UnnamedColumn predicate) {
        if (!having.isPresent()) {
            having = Optional.of(predicate);
        }
        else {
            having = Optional.<UnnamedColumn>of(ColumnOp.and(having.get(), predicate));
        }
    }

    public void addLimit(UnnamedColumn limit) {this.limit = Optional.of(limit); }

    public List<SelectItem> getSelectList() {
        return selectList;
    }

    public List<AbstractRelation> getFromList() {
        return fromList;
    }

    public Optional<UnnamedColumn> getFilter() {
        return filter;
    }
    
    public List<GroupingAttribute> getGroupby() {
        return groupby;
    }

    public List<OrderbyAttribute> getOrderby() {
        return orderby;
    }
    
    public Optional<String> getAliasName() {
        return aliasName;
    }

    public Optional<UnnamedColumn> getLimit() {return limit; }

    public Optional<UnnamedColumn> getHaving() {return having;}

}
