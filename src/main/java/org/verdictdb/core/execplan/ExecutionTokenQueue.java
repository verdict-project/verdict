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

package org.verdictdb.core.execplan;

import java.io.Serializable;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ExecutionTokenQueue implements Serializable {

  private static final long serialVersionUID = -1454513638973300702L;
  
  BlockingDeque<ExecutionInfoToken> internalQueue = new LinkedBlockingDeque<>();

  public void add(ExecutionInfoToken e) {
    internalQueue.add(e);
  }

  public ExecutionInfoToken poll() {
    return internalQueue.poll();
  }

  public synchronized ExecutionInfoToken take() {
    try {
      return internalQueue.takeFirst();
      //ExecutionInfoToken t = internalQueue.poll(5, TimeUnit.MINUTES);
      //if (t == null) {
      //  throw new RuntimeException("The internal token was unavailable for a long time for an unknown reasons.");
      //}
      //return t;
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE).toString();
  }

  public ExecutionInfoToken peek() {
    return internalQueue.peekFirst();
  }
}
