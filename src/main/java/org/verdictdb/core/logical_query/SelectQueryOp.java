package org.verdictdb.core.logical_query;

import java.util.ArrayList;
import java.util.List;

public class SelectQueryOp implements AbstractRelation {
    
    List<AbstractColumn> selectList = new ArrayList<>();
    
    List<AbstractRelation> fromList = new ArrayList<>();
    
    Predicate filter;
    
    public SelectQueryOp() {}
    
    public static SelectQueryOp getSelectQueryOp(List<AbstractColumn> columns, AbstractRelation relation) {
        SelectQueryOp sel = new SelectQueryOp();
        for (AbstractColumn c : columns) {
            sel.addSelectItem(c);
        }
        sel.addTableSource(relation);
        return sel;
    }
    
    public void addSelectItem(AbstractColumn column) {
        selectList.add(column);
    }
    
    public void addTableSource(AbstractRelation relation) {
        fromList.add(relation);
    }

    public List<AbstractColumn> getSelectList() {
        return selectList;
    }

    public List<AbstractRelation> getFromList() {
        return fromList;
    }

    public Predicate getFilter() {
        return filter;
    }

}
