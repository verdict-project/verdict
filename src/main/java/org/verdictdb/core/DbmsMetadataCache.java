package org.verdictdb.core;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

public interface DbmsMetadataCache {
  
//  public DbmsMetadataCache(DbmsConnection, SyntaxAbstract);
  
  public List<String> getSchemas();
  
  public List<String> getTables(String schema);
  
  public List<Pair<String, Integer>> getColumns(String schema, String table);
  
  /**
   * Only needed for the DBMS that supports partitioning.
   * 
   * @param schema
   * @param table
   * @return
   */
  public List<String> getPartitionColumns(String schema, String table);

}
