package org.verdictdb.core.logical_query;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.verdictdb.core.sql.syntax.SyntaxAbstract;
import org.verdictdb.exception.UnexpectedTypeException;


public class BaseTable extends AbstractRelation {
    
    String schemaName;
    
    String tableName;
    
    public BaseTable(String schemaName, String tableName, String tableSourceAlias) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        super.setAliasName(tableSourceAlias);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    
//    public String getTableSourceAlias() {
//        return tableSourceAlias;
//    }
//
//    public void setTableSourceAlias(String tableSourceAlias) {
//        this.tableSourceAlias = tableSourceAlias;
//    }

    public String toSql(SyntaxAbstract syntax) throws UnexpectedTypeException {
        throw new UnexpectedTypeException("A base table itself cannot be converted to a sql.");
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
