package org.verdictdb.core.logical_query;

import java.util.List;

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
     * <li>aggregate: aggregate column list + groupby list</li>
     * </ol>
     */
    Object parameters;
    
    public RelationalOp(AbstractRelation sourceRelation, String opType) {
        this.sourceRelation = sourceRelation;
        this.opType = opType;
    }

    @Override
    public String toSql(SyntaxAbstract syntax) throws VerdictDbException {
        String sql;
        if (opType.equals("aggregate")) {
            sql = toSqlAgg(syntax);
        } else {
            sql = null;
        }
        return sql;
    }
    
    private String toSqlAgg(SyntaxAbstract syntax) throws VerdictDbException {
        StringBuilder sql = new StringBuilder();
        
        // select
        sql.append("select");
        List<AbstractColumn> columns = (List<AbstractColumn>) parameters;
        for (AbstractColumn a : columns) {
            sql.append(" " + a.toSql(syntax));
        }
        
        // from
        sql.append(" from");
        sql.append(" " + sourceRelation.toSql(syntax));
        return sql.toString();
    }

}
