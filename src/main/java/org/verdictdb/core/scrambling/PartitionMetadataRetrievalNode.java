package org.verdictdb.core.scrambling;

import java.util.HashMap;
import java.util.Map;

import org.verdictdb.core.execution.MethodInvocationInformation;
import org.verdictdb.core.querying.ExecutableNodeBase;

public class PartitionMetadataRetrievalNode extends ExecutableNodeBase {
  
  /**
   * for which to retrieve metadata
   */
  private String schemaName; 
  
  /**
   * for which to retrieve metadata
   */
  private String tableName;
  
  /**
   * This key should be passed when specifying what methods should be called on DbmsConnection.
   */
  private String tokenKey;

  private PartitionMetadataRetrievalNode() {
    super();
  }
  
  static public PartitionMetadataRetrievalNode create(
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
        new MethodInvocationInformation("getPartitionColumns", 
            new Class<?>[] {String.class, String.class},
            new Object[] {schemaName, tableName});
    tokenKeyAndMethods.put(tokenKey, method);
    return tokenKeyAndMethods;
  }

}
