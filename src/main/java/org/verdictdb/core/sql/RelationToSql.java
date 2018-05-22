package org.verdictdb.core.sql;

import java.util.List;

import org.verdictdb.core.logical_query.AbstractColumn;
import org.verdictdb.core.logical_query.AbstractRelation;
import org.verdictdb.core.logical_query.BaseColumn;
import org.verdictdb.core.logical_query.BaseTable;
import org.verdictdb.core.logical_query.ColumnOp;
import org.verdictdb.core.logical_query.RelationalOp;
import org.verdictdb.core.sql.syntax.SyntaxAbstract;
import org.verdictdb.exception.UnexpectedTypeException;
import org.verdictdb.exception.VerdictDbException;

public class RelationToSql {
    
    SyntaxAbstract syntax;
    
    public RelationToSql(SyntaxAbstract syntax) {
        this.syntax = syntax;
    }
    
    public String toSql(AbstractRelation relation) throws VerdictDbException{
        if (relation instanceof BaseTable) {
            throw new UnexpectedTypeException("A base table itself cannot be converted to sql.");
        }
        
        return relationToSqlPart(relation);
    }
    
    String toSqlPart(AbstractColumn column) throws UnexpectedTypeException {
        if (column instanceof BaseColumn) {
            BaseColumn base = (BaseColumn) column;
            return quoteName(base.getTableSourceAlias()) + "." + quoteName(base.getColumnName());
        } else if (column instanceof AbstractColumn) {
            ColumnOp columnOp = (ColumnOp) column;
            if (columnOp.getOpType().equals("*")) {
                return "*";
            } else if (columnOp.getOpType().equals("avg")) {
                return "avg(" + toSqlPart(columnOp.getSource()) + ")";
            } else if (columnOp.getOpType().equals("sum")) {
                return "sum(" + toSqlPart(columnOp.getSource()) + ")";
            } else if (columnOp.getOpType().equals("count")) {
                return "count(" + toSqlPart(columnOp.getSource()) + ")";
            } else {
                throw new UnexpectedTypeException("Unexpceted opType of column: " + columnOp.getOpType().toString());
            }
        }
        throw new UnexpectedTypeException("Unexpceted argument type: " + column.getClass().toString());
    }
    
    String relationToSqlPart(AbstractRelation relation) throws VerdictDbException {
        StringBuilder sql = new StringBuilder();
        
        if (relation instanceof BaseTable) {
            BaseTable base = (BaseTable) relation;
            return quoteName(base.getSchemaName()) + "." + quoteName(base.getTableName());
        }
        
        RelationalOp rel = (RelationalOp) relation;
        if (rel.getOpType().equals("select") || rel.getOpType().equals("aggregate")) {
            // select
            sql.append("select");
            List<AbstractColumn> columns = (List<AbstractColumn>) rel.getParameters();
            boolean isFirstColumn = true;
            for (AbstractColumn a : columns) {
                if (isFirstColumn) {
                    sql.append(" " + toSqlPart(a));
                    isFirstColumn = false;
                } else {
                    sql.append(", " + toSqlPart(a));
                }
            }
            
            // from
            sql.append(" from");
            sql.append(" " + relationToSqlPart(rel.getSourceRelation()));
        } else {
            throw new UnexpectedTypeException("Unexpceted opType in relation: " + rel.getOpType());
        }
        return sql.toString();
    }
    
    String quoteName(String name) {
        String quoteString = syntax.getQuoteString();
        return quoteString + name + quoteString;
    }

}
