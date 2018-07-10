package org.verdictdb.core.coordinator;

import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.scrambling.ScrambleMeta;

import com.google.common.base.Optional;

public class ScramblingCoordinator {

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

  /**
   * Performs scrambling using a default method. Currently, the default method is 'uniform'.
   * 
   * @param originalSchema
   * @param originalTable
   * @return metadata information about scrambled table.
   */
  public ScrambleMeta scramble(String originalSchema, String originalTable) {
    String method = "uniform";
    ScrambleMeta meta = scramble(originalSchema, originalTable, method);
    return meta;
  }

  public ScrambleMeta scramble(String originalSchema, String originalTable, String method) {
    // should get assigned a new for the new table.
  
    return null;
  }

  public ScrambleMeta scramble(
      String originalSchema, String originalTable, 
      String newSchema, String newTable,
      String method) {

    return null;
  }

}
