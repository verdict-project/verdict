package org.verdictdb.core.querying;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertable;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;
import org.verdictdb.sqlreader.QueryToSql;

public class CreateTableAsSelectNode extends QueryNodeWithPlaceHolders {
  
  TempIdCreator namer;
  
  String newTableSchemaName;
  
  String newTableName;
  
  public CreateTableAsSelectNode(TempIdCreator namer, SelectQuery query) {
    super(query);
    this.namer = namer;
  }

  public static CreateTableAsSelectNode create(TempIdCreator namer, SelectQuery query) 
      throws VerdictDBValueException {
    CreateTableAsSelectNode node = new CreateTableAsSelectNode(namer, query);
    return node;
  }
  
  public TempIdCreator getNamer() {
    return namer;
  }
  
  @Override
  public SqlConvertable createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    super.createQuery(tokens);
    Pair<String, String> tempTableFullName = namer.generateTempTableName();
    newTableSchemaName = tempTableFullName.getLeft();
    newTableName = tempTableFullName.getRight();
    CreateTableAsSelectQuery createQuery = new CreateTableAsSelectQuery(newTableSchemaName, newTableName, selectQuery);
    return createQuery;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("schemaName", newTableSchemaName);
    token.setKeyValue("tableName", newTableName);
    return token;
  }

  @Override
  public BaseQueryNode deepcopy() {
    CreateTableAsSelectNode node = new CreateTableAsSelectNode(namer, selectQuery);
    copyFields(this, node);
    return node;
  }
  
  void copyFields(CreateTableAsSelectNode from, CreateTableAsSelectNode to) {
    super.copyFields(from, to);
  }

}
