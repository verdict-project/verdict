package org.verdictdb.core.logical_query;

import org.verdictdb.core.sql.syntax.SyntaxAbstract;
import org.verdictdb.exception.UnexpectedCallException;


public class BaseTable implements AbstractRelation {
    
    String schemaName;
    
    String tableName;
    
    public BaseTable(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public String toSql(SyntaxAbstract syntax) throws UnexpectedCallException {
        throw new UnexpectedCallException("A base table itself cannot be converted to a sql.");
    }
}
