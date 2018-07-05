package org.verdictdb.core.querying;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

public class CreateTableAsSelectNode extends QueryNodeWithPlaceHolders {
  
  IdCreator namer;
  
  String newTableSchemaName;
  
  String newTableName;
  
  public CreateTableAsSelectNode(IdCreator namer, SelectQuery query) {
    super(query);
    this.namer = namer;
  }

  public static CreateTableAsSelectNode create(IdCreator namer, SelectQuery query) {
    CreateTableAsSelectNode node = new CreateTableAsSelectNode(namer, query);
    return node;
  }
  
  public IdCreator getNamer() {
    return namer;
  }

  public void setNamer(TempIdCreator namer) {
    this.namer = namer;
  }
  
  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
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
  public ExecutableNodeBase deepcopy() {
    CreateTableAsSelectNode node = new CreateTableAsSelectNode(namer, selectQuery);
    copyFields(this, node);
    return node;
  }
  
  void copyFields(CreateTableAsSelectNode from, CreateTableAsSelectNode to) {
    super.copyFields(from, to);
  }

}
