package org.verdictdb.core.logical_query;

import java.util.HashMap;

import org.verdictdb.core.sql.syntax.SyntaxAbstract;
import org.verdictdb.exception.VerdictDbException;

public class RelationalOp implements AbstractRelation {
    
    AbstractRelation sourceRelation;
    
    /**
     * One of the following operations:
     * <ol>
     * <li>project</li>
     * <li>filter</li>
     * <li>aggregate</li>
     * </ol>
     */
    String opType;
    
    /**
     * Depending on the opType, this field has different roles:
     * <ol>
     * <li>project: projection column list</li>
     * <li>filter: a predicate</li>
     * <li>aggregate: aggregate column list</li>
     * </ol>
     */
    HashMap<String, Object> parameters;
    
    public RelationalOp(AbstractRelation sourceRelation, String opType) {
        this.sourceRelation = sourceRelation;
        this.opType = opType;
    }

    @Override
    public String toSql(SyntaxAbstract syntax) throws VerdictDbException {
        // TODO Auto-generated method stub
        return null;
    }
    
    private String toSqlAgg(SyntaxAbstract syntax) {
        return null;
    }

}
