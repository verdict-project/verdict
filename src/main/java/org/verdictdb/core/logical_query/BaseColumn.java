package org.verdictdb.core.logical_query;

public class BaseColumn extends UnnamedColumn implements SelectItem {
    
    String tableSourceAlias;
    
    String columnName;
    
    public String getTableSourceAlias() {
        return tableSourceAlias;
    }

    public void setTableSourceAlias(String tableSourceAlias) {
        this.tableSourceAlias = tableSourceAlias;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public BaseColumn(String tableSourceAlias, String columnName) {
        this.tableSourceAlias = tableSourceAlias;
        this.columnName = columnName;
    }
}
