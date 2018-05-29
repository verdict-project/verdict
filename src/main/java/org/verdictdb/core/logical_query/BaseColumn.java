package org.verdictdb.core.logical_query;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class BaseColumn implements UnnamedColumn, SelectItem, GroupingAttribute {
    
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
    
    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }
    
    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
