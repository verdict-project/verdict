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
