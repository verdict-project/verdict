package org.verdictdb.core.logical_query;

import org.verdictdb.core.sql.syntax.SyntaxAbstract;
import org.verdictdb.exception.VerdictDbException;

public class BaseColumn implements AbstractColumn {
    
    String columnName;
    
    public BaseColumn(String columnName) {
        this.columnName = columnName;
    }
    
    public String toSql(SyntaxAbstract syntax) throws VerdictDbException {
        String q = syntax.getQuoteString();
        return q + columnName + q;
    }

}
