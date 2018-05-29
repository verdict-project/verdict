package org.verdictdb.core.rewriter;

import org.verdictdb.exception.ValueException;

public class RewritingRules {
  
  public static String aggAliasName(String originalAliasName, String aggType) throws ValueException {
    if (aggType.equals("sum")) {
      return originalAliasName + "_sum";
    }
    else if (aggType.equals("count")) {
      return originalAliasName + "_count";
    }
    else if (aggType.equals("avg")) {
      return originalAliasName + "_avg";
    }
    else {
      throw new ValueException("Unexpected aggregate type: " + aggType);
    } 
  }
  
  public static String sumEstimateAliasName(String aliasName) {
    return "sum_" + aliasName;
  }
  
  public static String sumScaledSumAliasName(String aliasName) {
    return "sum_scaled_sum_" + aliasName;
  }
  
  public static String sumSquareScaledSumAliasName(String aliasName) {
    return "sum_square_scaled_sum_" + aliasName;
  }
  
  public static String countEstimateAliasName(String aliasName) {
    return "count_" + aliasName;
  }
  
  public static String sumScaledCountAliasName(String aliasName) {
    return "sum_scaled_count_" + aliasName;
  }
  
  public static String sumSquareScaledCountAliasName(String aliasName) {
    return "sum_square_scaled_count_" + aliasName;
  }
  
  public static String countSubsampleAliasName() {
    return "count_subsample";
  }
  
  public static String sumSubsampleSizeAliasName() {
    return "sum_subsample_size";
  }

}
