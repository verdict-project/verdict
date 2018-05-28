package org.verdictdb.core.logical_query;


import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the alias name that appears in the group-by clause or in the order-by clause.
 * This column does not include any reference to the table.
 * 
 * @author Yongjoo Park
 *
 */
public class AliasColumn implements GroupingAttribute, OrderbyAttribute {
    
    String column;
    String order = "asc";

    public AliasColumn(String column) {
        this.column = column;
    }

    // ascending decide the order in order-by clause
    public AliasColumn(String column, boolean ascending){
        this.column = column;
        if (!ascending) {
            this.order = "desc";
        }
    }
    
    public String getColumn() {
        return column;
    }

    public Pair<String, String> getOrderedColumn() {
        return new Pair<>(column, order);
    }


}
