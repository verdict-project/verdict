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

package org.verdictdb.core.scrambling;

import org.verdictdb.core.execplan.MethodInvocationInformation;
import org.verdictdb.core.querying.ExecutableNodeBase;

import java.util.HashMap;
import java.util.Map;

public class PartitionMetadataRetrievalNode extends ExecutableNodeBase {

  private static final long serialVersionUID = 3457736646345212051L;

  /** for which to retrieve metadata */
  private String schemaName;

  /** for which to retrieve metadata */
  private String tableName;

  /** This key should be passed when specifying what methods should be called on DbmsConnection. */
  private String tokenKey;

  private PartitionMetadataRetrievalNode() {
    super();
  }

  public static PartitionMetadataRetrievalNode create(
      String oldSchemaName, String oldTableName, String tokenKey) {
    PartitionMetadataRetrievalNode node = new PartitionMetadataRetrievalNode();
    node.schemaName = oldSchemaName;
    node.tableName = oldTableName;
    node.tokenKey = tokenKey;
    return node;
  }

  @Override
  public Map<String, MethodInvocationInformation> getMethodsToInvokeOnConnection() {
    Map<String, MethodInvocationInformation> tokenKeyAndMethods = new HashMap<>();
    MethodInvocationInformation method =
        new MethodInvocationInformation(
            "getPartitionColumns",
            new Class<?>[] {String.class, String.class},
            new Object[] {schemaName, tableName});
    tokenKeyAndMethods.put(tokenKey, method);
    return tokenKeyAndMethods;
  }
}
