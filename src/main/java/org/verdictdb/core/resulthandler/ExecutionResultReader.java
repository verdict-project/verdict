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

package org.verdictdb.core.resulthandler;

import java.util.Iterator;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.execplan.ExecutionTokenQueue;

public class ExecutionResultReader implements Iterable<DbmsQueryResult>, Iterator<DbmsQueryResult> {

  ExecutionTokenReader reader;

  public ExecutionResultReader() {}

  public ExecutionResultReader(ExecutionTokenReader reader) {
    this.reader = reader;
  }

  public ExecutionResultReader(ExecutionTokenQueue queue) {
    this(new ExecutionTokenReader(queue));
  }

  @Override
  public Iterator<DbmsQueryResult> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    return reader.hasNext();
  }

  @Override
  public DbmsQueryResult next() {
    ExecutionInfoToken token = reader.next();
    if (token == null) {
      return null;
    }
    return (DbmsQueryResult) token.getValue("queryResult");
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
