package org.verdictdb.core.scrambling;

import java.util.HashMap;
import java.util.Map;

import org.verdictdb.core.execplan.MethodInvocationInformation;
import org.verdictdb.core.querying.ExecutableNodeBase;

public class PrimaryKeyMetaDataRetrievalNode extends ExecutableNodeBase {

  private static final long serialVersionUID = 7457736646145212051L;

  /** for which to retrieve metadata */
  private String schemaName;

  /** for which to retrieve metadata */
  private String tableName;

  /** This key should be passed when specifying what methods should be called on DbmsConnection. */
  private String tokenKey;

  private PrimaryKeyMetaDataRetrievalNode() {
    super(-1);
  }

  public static PrimaryKeyMetaDataRetrievalNode create(
      String oldSchemaName, String oldTableName, String tokenKey) {
    PrimaryKeyMetaDataRetrievalNode node = new PrimaryKeyMetaDataRetrievalNode();
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
            "getPrimaryKey",
            new Class<?>[] {String.class, String.class},
            new Object[] {schemaName, tableName});
    tokenKeyAndMethods.put(tokenKey, method);
    return tokenKeyAndMethods;
  }
}
