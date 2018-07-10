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
  @SuppressWarnings("serial")
  Map<String, String> options = new HashMap<String, String>() {
    {
      put("tierColumnName", "verdictdbtier");
      put("blockColumnName", "verdictdbblock");
      put("scrambleTableSuffix", "_scrambled");
      put("scrambleTableBlockSize", "1e6");
    }
  };

  DbmsConnection conn;

  Optional<String> scrambleSchema;
  
  Optional<String> scratchpadSchema;
  
  public ScramblingCoordinator(DbmsConnection conn) {
    this(conn, null);
  }

  public ScramblingCoordinator(DbmsConnection conn, String scrambleSchema) {
    this(conn, scrambleSchema, scrambleSchema);           // uses the same schema
  }
  
  public ScramblingCoordinator(DbmsConnection conn, String scrambleSchema, String scratchpadSchema) {
    this(conn, scrambleSchema, scratchpadSchema, null);
  }
  
  public ScramblingCoordinator(DbmsConnection conn, String scrambleSchema, String scratchpadSchema, Long blockSize) {
    this.conn = conn;
    this.scrambleSchema = Optional.fromNullable(scrambleSchema);
    this.scratchpadSchema = Optional.fromNullable(scratchpadSchema);
    if (blockSize != null) {
      options.put("scrambleTableBlockSize", String.valueOf(blockSize));
    }
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
    ScramblingPlan plan = ScramblingPlan.create(newSchema, newTable, originalSchema, originalTable, scramblingMethod, options);
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
