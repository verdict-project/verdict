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
      String originalSchemaName, String originalTableName,
      String scrambledSchemaName, String scrambledTableName,
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
