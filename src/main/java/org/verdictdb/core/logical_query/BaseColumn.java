package org.verdictdb.core.logical_query;

import org.verdictdb.core.sql.syntax.SyntaxAbstract;
import org.verdictdb.exception.VerdictDbException;

public class BaseColumn implements AbstractColumn {
    
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
    
    public String toSql(SyntaxAbstract syntax) throws VerdictDbException {
        String q = syntax.getQuoteString();
        return q + columnName + q;
    }

}
