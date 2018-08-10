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

package org.verdictdb.coordinator;

/** Created by Dong Young Yoon on 8/8/18. */
public class QueryContext {
  private String verdictContextId;
  private Long executionSerialNumber;

  public QueryContext(String verdictContextId, Long executionSerialNumber) {
    this.verdictContextId = verdictContextId;
    this.executionSerialNumber = executionSerialNumber;
  }

  public String getVerdictContextId() {
    return verdictContextId;
  }

  public Long getExecutionSerialNumber() {
    return executionSerialNumber;
  }
}
