/*
 *    Copyright 2018 University of Michigan
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

package org.verdictdb.core.scrambling;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.IdCreator;
import org.verdictdb.core.querying.QueryNodeWithPlaceHolders;
import org.verdictdb.core.sqlobject.CreateScrambledTableQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;

import java.util.ArrayList;
import java.util.List;

/** Created by Dong Young Yoon on 7/17/18. */
public class CreateScrambledTableNode extends QueryNodeWithPlaceHolders {

  private static final long serialVersionUID = 1L;

  private IdCreator namer;

  protected String originalSchemaName;

  protected String originalTableName;

  protected String tierColumnName;

  protected String blockColumnName;

  private String newTableSchemaName;

  private String newTableName;

  private boolean createIfNotExists;

  protected List<String> partitionColumns = new ArrayList<>();

  protected ScramblingMethod method;

  protected UnnamedColumn predicate;

  public CreateScrambledTableNode(IdCreator namer, SelectQuery query) {
    super(namer, query);
    this.namer = namer;
  }

  public CreateScrambledTableNode(
      IdCreator namer,
      SelectQuery query,
      String originalSchemaName,
      String originalTableName,
      ScramblingMethod method,
      String tierColumnName,
      String blockColumnName,
      UnnamedColumn predicate,
      List<String> existingPartitionColumns,
      boolean createIfNotExists) {
    super(namer, query);
    this.namer = namer;
    this.originalSchemaName = originalSchemaName;
    this.originalTableName = originalTableName;
    this.method = method;
    this.tierColumnName = tierColumnName;
    this.blockColumnName = blockColumnName;
    this.predicate = predicate;
    this.partitionColumns.addAll(existingPartitionColumns);
    this.createIfNotExists = createIfNotExists;
  }

  public static CreateScrambledTableNode create(IdCreator namer, SelectQuery query) {
    return new CreateScrambledTableNode(namer, query);
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

  @SuppressWarnings("unchecked")
  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    super.createQuery(tokens);
    Pair<String, String> tempTableFullName = namer.generateTempTableName();
    newTableSchemaName = tempTableFullName.getLeft();
    newTableName = tempTableFullName.getRight();
    List<Pair<String, String>> columnMeta = null;
    List<String> primaryKeyColumnName = null;
    for (ExecutionInfoToken token : tokens) {
      Object val = token.getValue(ScramblingPlan.COLUMN_METADATA_KEY);
      if (val != null) {
        columnMeta = (List<Pair<String, String>>) val;
      } else {
        val = token.getValue(ScramblingPlan.PRIMARYKEY_METADATA_KEY);
        if (val != null) {
          primaryKeyColumnName = (List<String>) val;
        }
      }
    }
    if (columnMeta == null) {
      throw new VerdictDBException("Column meta is null.");
    }
    CreateScrambledTableQuery createQuery =
        new CreateScrambledTableQuery(
            originalSchemaName,
            originalTableName,
            newTableSchemaName,
            newTableName,
            tierColumnName,
            blockColumnName,
            selectQuery,
            method.getBlockCount(),
            method.getActualBlockCount(),
            columnMeta,
            primaryKeyColumnName,
            createIfNotExists);
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
    CreateScrambledTableNode node = new CreateScrambledTableNode(namer, selectQuery);
    copyFields(this, node);
    return node;
  }

  void copyFields(CreateScrambledTableNode from, CreateScrambledTableNode to) {
    super.copyFields(from, to);
  }
}
