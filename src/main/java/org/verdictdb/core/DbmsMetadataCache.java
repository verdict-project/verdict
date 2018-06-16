package org.verdictdb.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.JdbcResultSet;
import org.verdictdb.connection.DataTypeConverter;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.sql.syntax.SyntaxAbstract;

public class DbmsMetadataCache {

  private SyntaxAbstract syntax;

  private DbmsConnection connection;

  private List<String> schemaCache = new ArrayList<>();

  private List<String> tablesCache = new ArrayList<>();

  private List<String> partitionCache = new ArrayList<>();

  private List<Pair<String,Integer>> columnsCache = new ArrayList<>();

  public DbmsMetadataCache(DbmsConnection connection) {
    this.syntax = connection.getSyntax();
    this.connection = connection;
  }
  
  public List<String> getSchemas() throws SQLException {
    if (!schemaCache.isEmpty()){
      return schemaCache;
    }
    schemaCache.addAll(connection.getSchemas());
    return schemaCache;
  }
  
  public List<String> getTables(String schema) throws SQLException {
    if (!tablesCache.isEmpty()){
      return tablesCache;
    }
    tablesCache.addAll(connection.getTables(schema));
    return tablesCache;
  }

  public List<Pair<String, Integer>> getColumns(String schema, String table) throws SQLException {
    if (!columnsCache.isEmpty()){
      return columnsCache;
    }
    columnsCache.addAll(connection.getColumns(schema, table));
    return columnsCache;
  }
  
  /**
   * Only needed for the DBMS that supports partitioning.
   * 
   * @param schema
   * @param table
   * @return
   */
  public List<String> getPartitionColumns(String schema, String table) throws SQLException {
    if (!syntax.doesSupportTablePartitioning()) {
      throw new SQLException("Database does not support table partitioning");
    }
    if (!partitionCache.isEmpty()){
      return partitionCache;
    }
    partitionCache.addAll(getPartitionColumns(schema, table));
    return partitionCache;
  }

}
