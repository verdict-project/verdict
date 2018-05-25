package org.verdictdb.core.logical_query;

/**
 * Represents the alias name that appears in the group-by clause or in the order-by clause.
 * This column does not include any reference to the table.
 * 
 * @author Yongjoo Park
 *
 */
public class AliasColumn implements GroupingAttribute {
    
    String column;
    
    public AliasColumn(String column) {
        this.column = column;
    }
    
    public String getColumn() {
        return column;
    }

}
