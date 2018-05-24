package org.verdictdb.core.logical_query;

/**
 * Represents "column as aliasName".
 * 
 * @author Yongjoo Park
 *
 */
public class AliasedColumn implements SelectItem {
    
    UnnamedColumn column;
    
    String aliasName;
    
    public AliasedColumn(UnnamedColumn column, String aliasName) {
        this.column = column;
        this.aliasName = aliasName;
    }

    public UnnamedColumn getColumn() {
        return column;
    }

    public void setColumn(UnnamedColumn column) {
        this.column = column;
    }

    public String getAliasName() {
        return aliasName;
    }

    public void setAliasName(String aliasName) {
        this.aliasName = aliasName;
    }

}
