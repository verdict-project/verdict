package org.verdictdb.core.rewriter;

public class AliasRenamingRules {
  
  public static String expectedValueAliasName(String aliasName) {
    return aliasName;
  }
  
  public static String expectedErrorAliasName(String aliasName) {
    return aliasName + ":verdictdb:err";
  }

  // internal aliases
  public static String sumEstimateAliasName(String aliasName) {
    return aliasName + ":verdictdb:sum";
  }
  
  public static String countEstimateAliasName(String aliasName) {
    return aliasName + ":verdictdb:count";
  }
  
  public static String sumScaledSumAliasName(String aliasName) {
    return aliasName + ":verdictdb:sum_scaled_su,";
  }
  
  public static String sumSquaredScaledSumAliasName(String aliasName) {
    return aliasName + ":verdictdb:sum_squared_scaled_sum";
  }
  
  public static String sumScaledCountAliasName(String aliasName) {
    return aliasName + ":verdictdb:sum_scaled_count";
  }
  
  public static String sumSquaredScaledCountAliasName(String aliasName) {
    return aliasName + ":sum_squared_scaled_count";
  }
  
  public static String countSubsampleAliasName() {
    return "verdictdb:count_subsample";
  }
  
  public static String sumSubsampleSizeAliasName() {
    return "verdictdb:sum_subsample_size";
  }
  
  public static String tierAliasName() {
    return "verdictdb:tier";
  }

}
