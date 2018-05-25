package org.verdictdb.core.logical_query;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SelectQueryOp implements AbstractRelation {
    
    List<SelectItem> selectList = new ArrayList<>();
    
    List<AbstractRelation> fromList = new ArrayList<>();
    
    Optional<UnnamedColumn> filter = Optional.empty();
    
    List<GroupingAttribute> groupby = new ArrayList<>();
    
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
    
    public Optional<String> getAliasName() {
        return aliasName;
    }

}
