package org.verdictdb.core.scramble;

import org.verdictdb.core.ScrambleMetaForTable;

public class Scrambler {
  
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
  
  public Scrambler(
      String originalSchemaName, String originalTableName,
      String scrambledSchemaName, String scrambledTableName,
      int aggregationBlockCount) {
    this.originalSchemaName = originalSchemaName;
    this.originalTableName = originalTableName;
    this.scrambledSchemaName = scrambledSchemaName;
    this.scrambledTableName = scrambledTableName;
    this.aggregationBlockCount = aggregationBlockCount;
  }
  
  public ScrambleMetaForTable generateMeta() {
    ScrambleMetaForTable meta = new ScrambleMetaForTable();
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
