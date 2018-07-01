package org.verdictdb.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.DbmsConnection;
import org.verdictdb.MetaDataProvider;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sql.syntax.SqlSyntax;

public class DbmsMetaDataCache implements MetaDataProvider {

  private SqlSyntax syntax;

  private DbmsConnection connection;

  private List<String> schemaCache = new ArrayList<>();

  private HashMap<String, List<String>> tablesCache = new HashMap<>();

  private HashMap<Pair<String, String>, List<String>> partitionCache = new HashMap<>();

  private HashMap<Pair<String, String>, List<Pair<String,Integer>>> columnsCache = new HashMap<>();

  public DbmsMetaDataCache(DbmsConnection connection) {
    this.syntax = connection.getSyntax();
    this.connection = connection;
  }

  @Override
  public List<String> getSchemas() throws VerdictDBDbmsException {
    if (!schemaCache.isEmpty()) {
      return schemaCache;
    }
    schemaCache.addAll(connection.getSchemas());
    return schemaCache;
  }

  @Override
  public List<String> getTables(String schema) throws VerdictDBDbmsException {
    if (tablesCache.containsKey(schema)&&!tablesCache.get(schema).isEmpty()) {
      return tablesCache.get(schema);
    }
    tablesCache.put(schema, connection.getTables(schema));
    return tablesCache.get(schema);
  }

  @Override
  public List<Pair<String, Integer>> getColumns(String schema, String table) throws VerdictDBDbmsException {
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
   * @throws VerdictDBDbmsException 
   */
  @Override
  public List<String> getPartitionColumns(String schema, String table) throws VerdictDBDbmsException {
    if (!syntax.doesSupportTablePartitioning()) {
      throw new VerdictDBDbmsException("Database does not support table partitioning");
    }
    Pair<String, String> key = new ImmutablePair<>(schema,table);
    if (columnsCache.containsKey(key) && !partitionCache.isEmpty()) {
      return partitionCache.get(key);
    }
    partitionCache.put(key, connection.getPartitionColumns(schema, table));
    return partitionCache.get(key);
  }

  @Override
  public String getDefaultSchema() {
    return connection.getDefaultSchema();
  }

}
