package org.verdictdb.core.resultset;

import tech.tablesaw.api.Table;

public class BlockAggregateSet {
  
  Table rawdata;
  
  /**
   * After performing proper error computations
   */
  Table convertedResultset;
  
  public BlockAggregateSet(Table rawdata, Table converted) {
    this.rawdata = rawdata;
    this.convertedResultset = converted;
  }

}
