package org.verdictdb.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.sql.syntax.SyntaxAbstract;

public class DbmsMetadataCache {

  private SyntaxAbstract syntax;

  private DbmsConnection connection;

  private List<String> schemaCache = new ArrayList<>();

  private HashMap<String, List<String>> tablesCache = new HashMap<>();

  private HashMap<Pair<String, String>, List<String>> partitionCache = new HashMap<>();

  private HashMap<Pair<String, String>, List<Pair<String,Integer>>> columnsCache = new HashMap<>();

  public DbmsMetadataCache(DbmsConnection connection) {
    this.syntax = connection.getSyntax();
    this.connection = connection;
  }

  public List<String> getSchemas() throws SQLException {
    if (!schemaCache.isEmpty()) {
      return schemaCache;
    }
    schemaCache.addAll(connection.getSchemas());
    return schemaCache;
  }

  public List<String> getTables(String schema) throws SQLException {
    if (tablesCache.containsKey(schema)&&!tablesCache.get(schema).isEmpty()) {
      return tablesCache.get(schema);
    }
    tablesCache.put(schema, connection.getTables(schema));
    return tablesCache.get(schema);
  }

  public List<Pair<String, Integer>> getColumns(String schema, String table) throws SQLException {
    Pair<String, String> key = new ImmutablePair<>(schema,table);
    if (columnsCache.containsKey(key) && !columnsCache.get(key).isEmpty()) {
      return columnsCache.get(key);
    }
    columnsCache.put(key, connection.getColumns(schema, table));
    return columnsCache.get(key);
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
    Pair<String, String> key = new ImmutablePair<>(schema,table);
    if (columnsCache.containsKey(key) && !partitionCache.isEmpty()) {
      return partitionCache.get(key);
    }
    partitionCache.put(key, connection.getPartitionColumns(schema, table));
    return partitionCache.get(key);
  }

}
