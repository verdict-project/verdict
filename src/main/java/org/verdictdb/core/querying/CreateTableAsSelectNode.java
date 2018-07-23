/*
 *    Copyright 2017 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.core.querying;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

import java.util.ArrayList;
import java.util.List;

public class CreateTableAsSelectNode extends QueryNodeWithPlaceHolders {

  private static final long serialVersionUID = -8722221355083655181L;

  IdCreator namer;

  String newTableSchemaName;

  String newTableName;

  List<String> partitionColumns = new ArrayList<>();

  boolean ifNotExists = false;

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

  public void setNamer(IdCreator namer) {
    this.namer = namer;
  }

  public void addPartitionColumn(String column) {
    partitionColumns.add(column);
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    super.createQuery(tokens);
    Pair<String, String> tempTableFullName = namer.generateTempTableName();
    newTableSchemaName = tempTableFullName.getLeft();
    newTableName = tempTableFullName.getRight();
    CreateTableAsSelectQuery createQuery =
        new CreateTableAsSelectQuery(newTableSchemaName, newTableName, selectQuery);
    for (String col : partitionColumns) {
      createQuery.addPartitionColumn(col);
    }
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
