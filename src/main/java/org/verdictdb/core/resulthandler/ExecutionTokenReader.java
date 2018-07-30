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

import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.execplan.ExecutionTokenQueue;

public class ExecutionTokenReader
    implements Iterable<ExecutionInfoToken>, Iterator<ExecutionInfoToken> {

  ExecutionTokenQueue queue;

  // set to true if the status token has been taken from "queue".
  boolean hasEndOfQueueReached = false;

  ExecutionInfoToken queueBuffer = null;

  public ExecutionTokenReader() {}

  public ExecutionTokenReader(ExecutionTokenQueue queue) {
    this.queue = queue;
  }

  @Override
  public Iterator<ExecutionInfoToken> iterator() {
    return this;
  }

  public void takeOne() {
    queueBuffer = queue.take();

    if (queueBuffer.isFailureToken()) {
      Exception e = (Exception) queueBuffer.getValue("errorMessage");
      if (e != null) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (queue == null) {
      return false;
    }

    if (queueBuffer == null) {
      takeOne();
      return hasNext();
    }

    if (queueBuffer.isStatusToken()) {
      return false;
    } else {
      return true;
    }
  }

  @Override
  public ExecutionInfoToken next() {
    if (queue == null) {
      return null;
    }

    if (queueBuffer == null) {
      takeOne();
      return next();
    }

    if (queueBuffer.isStatusToken()) {
      return null;
    } else {
      ExecutionInfoToken result = queueBuffer;
      queueBuffer = null;
      return result;
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
