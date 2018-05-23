package org.verdictdb.core.logical_query;

import java.util.ArrayList;
import java.util.List;

public class SelectQueryOp implements AbstractRelation {
    
    List<AbstractColumn> selectList = new ArrayList<>();
    
    List<AbstractRelation> fromList = new ArrayList<>();
    
    Predicate filter;
    
    public SelectQueryOp() {}
    
    public void addSelectItem(AbstractColumn column) {
        selectList.add(column);
    }
    
    public void addTableSource(AbstractRelation relation) {
        fromList.add(relation);
    }

}
