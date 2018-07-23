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

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.DropTableQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

import java.util.List;

public class DropTableExecutionNode extends ExecutableNodeBase {

  public DropTableExecutionNode() {
    super();
  }

  public static DropTableExecutionNode create() {
    DropTableExecutionNode node = new DropTableExecutionNode();
    return node;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    try {
      if (tokens.size() == 0) {
        throw new VerdictDBValueException("No table to drop!");
      }
    } catch (VerdictDBException e) {
      e.printStackTrace();
    }

    ExecutionInfoToken result = tokens.get(0);
    String schemaName = (String) result.getValue("schemaName");
    String tableName = (String) result.getValue("tableName");
    DropTableQuery dropQuery = new DropTableQuery(schemaName, tableName);
    return dropQuery;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    return ExecutionInfoToken.empty();
  }

  @Override
  public ExecutableNodeBase deepcopy() {
    DropTableExecutionNode node = new DropTableExecutionNode();
    copyFields(this, node);
    return node;
  }

  void copyFields(DropTableExecutionNode from, DropTableExecutionNode to) {
    super.copyFields(from, to);
  }
}
