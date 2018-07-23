package org.verdictdb.connection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.exception.VerdictDBDbmsException;

public class CachedMetaDataProvider implements MetaDataProvider {
  
  MetaDataProvider metaProvider;
  
//  private String defaultSchema = null;

  private List<String> schemaCache = new ArrayList<>();

  private HashMap<String, List<String>> tablesCache = new HashMap<>();

  private HashMap<Pair<String, String>, List<String>> partitionCache = new HashMap<>();

  // Get column name and type
  private HashMap<Pair<String, String>, List<Pair<String,String>>> columnsCache = new HashMap<>();

  public CachedMetaDataProvider(MetaDataProvider metaProvider) {
    this.metaProvider = metaProvider;
  }

  @Override
  public List<String> getSchemas() throws VerdictDBDbmsException {
    if (!schemaCache.isEmpty()) {
      return schemaCache;
    }
    schemaCache.addAll(metaProvider.getSchemas());
    return schemaCache;
  }

  @Override
  public List<String> getTables(String schema) throws VerdictDBDbmsException {
    if (tablesCache.containsKey(schema)&&!tablesCache.get(schema).isEmpty()) {
      return tablesCache.get(schema);
    }
    tablesCache.put(schema, metaProvider.getTables(schema));
    return tablesCache.get(schema);
  }

  @Override
  public List<Pair<String, String>> getColumns(String schema, String table) throws VerdictDBDbmsException {
    Pair<String, String> key = new ImmutablePair<>(schema,table);
    if (columnsCache.containsKey(key) && !columnsCache.get(key).isEmpty()) {
      return columnsCache.get(key);
    }
    columnsCache.put(key, metaProvider.getColumns(schema, table));
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
//    if (!syntax.doesSupportTablePartitioning()) {
//      throw new VerdictDBDbmsException("Database does not support table partitioning");
//    }
    Pair<String, String> key = new ImmutablePair<>(schema,table);
    if (columnsCache.containsKey(key) && !partitionCache.isEmpty()) {
      return partitionCache.get(key);
    }
    partitionCache.put(key, metaProvider.getPartitionColumns(schema, table));
    return partitionCache.get(key);
  }
  
  public String getDefaultSchema() {
    String schema = metaProvider.getDefaultSchema();
//    if (defaultSchema == null) {
//      defaultSchema = metaProvider.getDefaultSchema();
//    }
    return schema;
  }
  
  public void setDefaultSchema(String schema) {
    metaProvider.setDefaultSchema(schema);
//    defaultSchema = schema;
  }


}
