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

package org.verdictdb.core.scrambling;

public class BaseScrambler {

  String originalSchemaName;

  String originalTableName;

  String scrambledSchemaName;

  String scrambledTableName;

  int aggregationBlockCount;

  // Configuration parameters
  static String aggregationBlockColumn = "verdictdbaggblock";

  //  static String inclusionProbabilityColumn = "verdictIncProb";
  //
  //  static String inclusionProbabilityBlockDifferenceColumn = "verdictIncProbBlockDiff";

  static String subsampleColumn = "verdictdbsid";

  static String tierColumn = "verdictdbtier";

  public BaseScrambler(
      String originalSchemaName,
      String originalTableName,
      String scrambledSchemaName,
      String scrambledTableName,
      int aggregationBlockCount) {
    this.originalSchemaName = originalSchemaName;
    this.originalTableName = originalTableName;
    this.scrambledSchemaName = scrambledSchemaName;
    this.scrambledTableName = scrambledTableName;
    this.aggregationBlockCount = aggregationBlockCount;
  }

  public ScrambleMeta generateMeta() {
    ScrambleMeta meta = new ScrambleMeta();
    meta.setSchemaName(scrambledSchemaName);
    meta.setTableName(scrambledTableName);
    meta.setAggregationBlockCount(aggregationBlockCount);
    meta.setAggregationBlockColumn(aggregationBlockColumn);
    meta.setSubsampleColumn(subsampleColumn);
    meta.setTierColumn(tierColumn);
    return meta;
  }

  public static String getAggregationBlockColumn() {
    return aggregationBlockColumn;
  }

  //  public static String getInclusionProbabilityColumn() {
  //    return inclusionProbabilityColumn;
  //  }

  //  public static String getInclusionProbabilityBlockDifferenceColumn() {
  //    return inclusionProbabilityBlockDifferenceColumn;
  //  }

  public static String getSubsampleColumn() {
    return subsampleColumn;
  }

  public static String getTierColumn() {
    return tierColumn;
  }
}
