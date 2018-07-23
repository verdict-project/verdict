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

package org.verdictdb.coordinator;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.resulthandler.ExecutionResultReader;

import java.util.Iterator;

public class VerdictResultStream
    implements Iterable<VerdictSingleResult>, Iterator<VerdictSingleResult> {

  ExecutionResultReader reader;

  ExecutionContext execContext;

  public VerdictResultStream(ExecutionResultReader reader, ExecutionContext execContext) {
    this.reader = reader;
    this.execContext = execContext;
  }

  // TODO
  public VerdictResultStream create(VerdictSingleResult singleResult) {
    return null;
  }

  @Override
  public boolean hasNext() {
    return reader.hasNext();
  }

  @Override
  public VerdictSingleResult next() {
    DbmsQueryResult internalResult = reader.next();
    VerdictSingleResult result = new VerdictSingleResult(internalResult);
    return result;
  }

  @Override
  public Iterator<VerdictSingleResult> iterator() {
    return this;
  }

  @Override
  public void remove() {}

  public void close() {}
}
