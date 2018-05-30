package org.verdictdb.core.sql;

import java.util.List;

import org.verdictdb.core.logical_query.CreateTableAsSelect;
import org.verdictdb.core.logical_query.SelectQueryOp;
import org.verdictdb.core.sql.syntax.SyntaxAbstract;
import org.verdictdb.exception.VerdictDbException;

public class CreateTableToSql {
  
  SyntaxAbstract syntax;

  public CreateTableToSql(SyntaxAbstract syntax) {
    this.syntax = syntax;
  }
  
  public String toSql(CreateTableAsSelect query) throws VerdictDbException{
    StringBuilder sql = new StringBuilder();
    
    String schemaName = query.getSchemaName();
    String tableName = query.getTableName();
    SelectQueryOp select = query.getSelect();
    
    // table
    sql.append("create table ");
    sql.append(quoteName(schemaName));
    sql.append(".");
    sql.append(quoteName(tableName));
    
    // partitions
    if (syntax.doesSupportTablePartitioning() && query.getPartitionColumns().size() > 0) {
      sql.append(" partitioned by (");
      List<String> partitionColumns = query.getPartitionColumns();
      boolean isFirstColumn = true;
      for (String col : partitionColumns) {
        if (isFirstColumn) {
          sql.append(col);
          isFirstColumn = false;
        }
        else {
          sql.append(", " + col);
        }
      }
      sql.append(")");
    }
    
    // select
    sql.append(" as ");
    SelectQueryToSql selectWriter = new SelectQueryToSql(syntax);
    String selectSql = selectWriter.toSql(select);
    sql.append(selectSql);
    
    return sql.toString();
  }
  
  String quoteName (String name){
    String quoteString = syntax.getQuoteString();
    return quoteString + name + quoteString;
  }

}
