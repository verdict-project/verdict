package org.verdictdb.core.rewriter.query;

public class AliasRenamingRules {
  
  public static String expectedValueAliasName(String aliasName) {
    return aliasName;
  }
  
  public static String expectedErrorAliasName(String aliasName) {
    return aliasName + "_err";
  }

  // internal aliases
  public static String sumEstimateAliasName(String aliasName) {
    return "sum_" + aliasName;
  }
  
  public static String countEstimateAliasName(String aliasName) {
    return "count_" + aliasName;
  }
  
  public static String sumScaledSumAliasName(String aliasName) {
    return "sum_scaled_sum_" + aliasName;
  }
  
  public static String sumSquaredScaledSumAliasName(String aliasName) {
    return "sum_squared_scaled_sum_" + aliasName;
  }
  
  public static String sumScaledCountAliasName(String aliasName) {
    return "sum_scaled_count_" + aliasName;
  }
  
  public static String sumSquaredScaledCountAliasName(String aliasName) {
    return "sum_squared_scaled_count_" + aliasName;
  }
  
  public static String countSubsampleAliasName() {
    return "count_subsample";
  }
  
  public static String sumSubsampleSizeAliasName() {
    return "sum_subsample_size";
  }
  
  public static String tierAliasName() {
    return "verdicttier";
  }

}
