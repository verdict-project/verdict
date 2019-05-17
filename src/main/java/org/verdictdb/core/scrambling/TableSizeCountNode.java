package org.verdictdb.core.scrambling;

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

class TableSizeCountNode extends QueryNodeBase {

  private static final long serialVersionUID = 4363953197389542868L;

  private String schemaName;

  private String tableName;
  
  private UnnamedColumn predicate = null;

  public static final String TOTAL_COUNT_ALIAS_NAME = "verdictdbtotalcount";

  public TableSizeCountNode(String schemaName, String tableName) {
    super(-1, null);
    this.schemaName = schemaName;
    this.tableName = tableName;
  }
  
  public TableSizeCountNode(String schemaName, String tableName, UnnamedColumn predicate) {
    super(-1, null);
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.predicate = predicate;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    if (tokens.size() == 0) {
      // no token information passed
      throw new VerdictDBValueException("No token is passed.");
    }

    String tableSourceAlias = "t";

    // compose a select list
    List<SelectItem> selectList = new ArrayList<>();
    selectList.add(new AliasedColumn(ColumnOp.count(), TOTAL_COUNT_ALIAS_NAME));

    if (predicate == null) {
      selectQuery =
          SelectQuery.create(selectList, new BaseTable(schemaName, tableName, tableSourceAlias));
    } else {
      selectQuery =
          SelectQuery.create(
              selectList, 
              new BaseTable(schemaName, tableName, tableSourceAlias), 
              predicate);
    }
    return selectQuery;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue(this.getClass().getSimpleName(), result);
    return token;
  }
}