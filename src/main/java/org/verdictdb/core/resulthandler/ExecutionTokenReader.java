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

import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.execplan.ExecutionTokenQueue;

public class ExecutionTokenReader
    implements Iterable<ExecutionInfoToken>, Iterator<ExecutionInfoToken> {

  ExecutionTokenQueue queue;

  ExecutionInfoToken queueBuffer = null;
  
  private VerdictDBLogger log = VerdictDBLogger.getLogger(getClass());

  public ExecutionTokenReader() {}

  public ExecutionTokenReader(ExecutionTokenQueue queue) {
    this.queue = queue;
  }

  @Override
  public Iterator<ExecutionInfoToken> iterator() {
    return this;
  }

  public void takeOne() {
    log.trace("Attempts to take a result.");
    queueBuffer = queue.take();

    if (queueBuffer.isFailureToken()) {
      Exception e = (Exception) queueBuffer.getValue("errorMessage");
      if (e != null) {
        log.trace("An runtime error is being thrown from ExecutionTokenReader.");
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean hasNext() {
    if (queue == null) {
      return false;
    }

    synchronized (this) {
      if (queueBuffer == null) {
        takeOne();
        return hasNext();
      }
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

    synchronized (this) {
      if (queueBuffer == null) {
        takeOne();
        return next();
      }
    }

    if (queueBuffer.isStatusToken()) {
      log.trace("A status token is read: " + queueBuffer);
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
