package org.verdictdb.core.scrambling;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.verdictdb.exception.VerdictDBValueException;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Table-specific information
 * @author Yongjoo Park
 *
 */
public class ScrambleMeta implements Serializable {

  private static final long serialVersionUID = -8422601151874567149L;

  // key
  String schemaName;

  String tableName;

  // aggregation block
  String aggregationBlockColumn;        // agg block number (0 to count-1)

  int aggregationBlockCount;            // agg block total count

  // tier
  String tierColumn;
  
  int numberOfTiers;
  
  // reference to the original tables
  String originalSchemaName;
  
  String originalTableName;
  
  /**
   * The probability mass function of the sizes of the aggregation blocks for a tier.
   * The key is the id of a tier (e.g., 0, 1, ..., 3), and the list is the cumulative distribution.
   * The length of the cumulative distribution must be equal to aggregationBlockCount.
   */
  @JsonProperty("cumulativeMassDistributionPerTier")
  Map<Integer, List<Double>> cumulativeMassDistributionPerTier = new HashMap<>();
  
  // subsample column; not used currently
  String subsampleColumn;

  public ScrambleMeta() {}
  
  public ScrambleMeta(
      String scrambleSchemaName, String scrambleTableName,
      String originalSchemaName, String originalTableName,
      String blockColumn, int blockCount,
      String tierColumn, int tierCount,
      Map<Integer, List<Double>> cumulativeMassDistributionPerTier) 
          throws VerdictDBValueException {
    
    if (tierCount != cumulativeMassDistributionPerTier.size()) {
      throw new VerdictDBValueException("The number of tiers don't match.");
    }
    for (Entry<Integer, List<Double>> p : cumulativeMassDistributionPerTier.entrySet()) {
      List<Double> dist = p.getValue();
      if (dist == null) {
        throw new VerdictDBValueException("NULL is passed for a cumulative distribution.");
      }
      if (blockCount != dist.size()) {
        throw new VerdictDBValueException("The number of blocks don't match.");
      }
    }
    
    this.schemaName = scrambleSchemaName;
    this.tableName = scrambleTableName;
    this.aggregationBlockColumn = blockColumn;
    this.aggregationBlockCount = blockCount;
    this.tierColumn = tierColumn;
    this.numberOfTiers = tierCount;
    this.originalSchemaName = originalSchemaName;
    this.originalTableName = originalTableName;
    this.cumulativeMassDistributionPerTier = cumulativeMassDistributionPerTier;
  }
  
  public String getSchemaName() {
    return schemaName;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getAggregationBlockColumn() {
    return aggregationBlockColumn;
  }

  public void setAggregationBlockColumn(String aggregationBlockColumn) {
    this.aggregationBlockColumn = aggregationBlockColumn;
  }

  public int getAggregationBlockCount() {
    return aggregationBlockCount;
  }

  public void setAggregationBlockCount(int aggregationBlockCount) {
    this.aggregationBlockCount = aggregationBlockCount;
  }

  public String getTierColumn() {
    return tierColumn;
  }

  public void setTierColumn(String tierColumn) {
    this.tierColumn = tierColumn;
  }

  public String getSubsampleColumn() {
    return subsampleColumn;
  }
  
  public List<Double> getCumulativeProbabilityDistribution(int tier) {
    return cumulativeMassDistributionPerTier.get(tier);
  }

  public void setSubsampleColumn(String subsampleColumn) {
    this.subsampleColumn = subsampleColumn;
  }

  public int getNumberOfTiers() {
    return numberOfTiers;
  }

  public void setCumulativeMassDistributionPerTier(Map<Integer, List<Double>> cumulativeMassDistributionPerTier) {
    this.cumulativeMassDistributionPerTier = cumulativeMassDistributionPerTier;
  }

  public void setNumberOfTiers(int numberOfTiers) {
    this.numberOfTiers = numberOfTiers;
  }
}

