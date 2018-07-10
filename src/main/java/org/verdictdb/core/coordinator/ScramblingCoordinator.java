package org.verdictdb.core.coordinator;

import java.util.HashMap;
import java.util.Map;

import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.execution.ExecutablePlanRunner;
import org.verdictdb.core.scrambling.FastConvergeScramblingMethod;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScramblingMethod;
import org.verdictdb.core.scrambling.ScramblingPlan;
import org.verdictdb.core.scrambling.UniformScramblingMethod;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

public class ScramblingCoordinator {
  
  // default options
  // some key names (tierColumnName, blockColumnName) are also used by ScramblingNode; thus, they cannot
  // be modified arbitrarily.
  Map<String, String> options = ImmutableMap.<String, String>builder()
      .put("tierColumnName", "verdictdbtier")
      .put("blockColumnName", "verdictdbblock")
      .put("scrambleTableSuffix", "_scrambled")
      .put("scrambleTableBlockSize", "1e6")
      .build();

  DbmsConnection conn;

  Optional<String> scrambleSchema;
  
  Optional<String> scratchpadSchema;
  
  public ScramblingCoordinator() {
    scrambleSchema = Optional.absent();
  }

  public ScramblingCoordinator(String scrambleSchema) {
    this.scrambleSchema = Optional.of(scrambleSchema);
    this.scratchpadSchema = Optional.of(scrambleSchema);    // uses the same schema
  }
  
  public ScramblingCoordinator(String scrambleSchema, String scratchpadSchema) {
    this.scrambleSchema = Optional.of(scrambleSchema);
    this.scratchpadSchema = Optional.of(scratchpadSchema);
  }
  
  public ScramblingCoordinator(String scrambleSchema, String scratchpadSchema, long blockSize) {
    this.scrambleSchema = Optional.of(scrambleSchema);
    this.scratchpadSchema = Optional.of(scratchpadSchema);
    options.put("scrambleTableBlockSize", String.valueOf(blockSize));
  }

  /**
   * Performs scrambling using a default method. Currently, the default method is 'uniform'.
   * 
   * @param originalSchema
   * @param originalTable
   * @return metadata information about scrambled table.
   * @throws VerdictDBException 
   */
  public ScrambleMeta scramble(String originalSchema, String originalTable) throws VerdictDBException {
    String method = "uniform";
    ScrambleMeta meta = scramble(originalSchema, originalTable, method);
    return meta;
  }

  public ScrambleMeta scramble(String originalSchema, String originalTable, String method) 
      throws VerdictDBException {
    
    // should get assigned a new scratchpad schema for new tables.
    String newSchema;
    if (scrambleSchema.isPresent()) {
      newSchema = scrambleSchema.get();
    } else {
      newSchema = originalSchema;
    }
    
    String newTable = originalTable + options.get("scrambleTableSuffix");
    
    ScrambleMeta meta = scramble(originalSchema, originalTable, newSchema, newTable, method);
    return meta;
  }

  public ScrambleMeta scramble(
      String originalSchema, String originalTable, 
      String newSchema, String newTable,
      String methodName) throws VerdictDBException {
    
    // determine scrambling method
    ScramblingMethod scramblingMethod;
    long blockSize = Double.valueOf(options.get("scrambleTableBlockSize")).longValue();
    if (methodName.equalsIgnoreCase("uniform")) {
      scramblingMethod = new UniformScramblingMethod(blockSize);
    } else if (methodName.equalsIgnoreCase("FastConverge")) {
      scramblingMethod = new FastConvergeScramblingMethod(blockSize, scratchpadSchema.get());
    } else {
      throw new VerdictDBValueException("Invalid scrambling method: " + methodName);
    }
    
    // perform scrambling
    ScramblingPlan plan = ScramblingPlan.create(originalSchema, originalTable, newSchema, newTable, scramblingMethod, options);
    ExecutablePlanRunner.runTillEnd(conn, plan);
    
    // compose scramble meta
    String blockColumn = options.get("blockColumnName");
    int blockCount = scramblingMethod.getBlockCount();
    String tierColumn = options.get("tierColumnName");
    int tierCount = scramblingMethod.getTierCount();
    ScrambleMeta meta = new ScrambleMeta(newSchema, newTable, blockColumn, blockCount, tierColumn, tierCount, originalSchema, originalTable);

    return meta;
  }

}
