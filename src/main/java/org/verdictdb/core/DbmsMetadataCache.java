package org.verdictdb.core;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.JdbcDriver;
import org.verdictdb.connection.DataTypeConverter;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.sql.syntax.SyntaxAbstract;

public class DbmsMetadataCache {

  private SyntaxAbstract syntax;

  private DbmsConnection connection;

  private List<String> schemaCache = new ArrayList<>();

  // Key is schema name and value is table names in this schema.
  private HashMap<String, List<String>> tablesCache = new HashMap<>();

  private List<String> partitionCache = new ArrayList<>();

  // First Key is schema name and second key is table name.
  // Value is columns' names and types in this table.
  private HashMap<Pair<String, String>, List<Pair<String,Integer>>> columnsCache = new HashMap<>();

  public DbmsMetadataCache(DbmsConnection connection) {
    this.syntax = connection.getSyntax();
    this.connection = connection;
  }
  
  public List<String> getSchemas() throws SQLException {
    DbmsQueryResult queryResult = connection.executeQuery(syntax.getSchemaCommand());
    JdbcDriver jdbcQueryResult = new JdbcDriver(queryResult);
    List<String> schemas = new ArrayList<>();
    while (queryResult.next()) {
      schemas.add(jdbcQueryResult.getString(syntax.getSchemaNameColumnName()));
    }
    schemaCache.addAll(schemas);
    return schemas;
  }
  
  public List<String> getTables(String schema) throws SQLException {
    DbmsQueryResult queryResult = connection.executeQuery(syntax.getTableCommand(schema));
    JdbcDriver jdbcQueryResult = new JdbcDriver(queryResult);
    List<String> tables = new ArrayList<>();
    while (queryResult.next()) {
      tables.add(jdbcQueryResult.getString(syntax.getTableNameColumnName()));
    }
    tablesCache.put(schema, tables);
    return tables;
  }
  
  public List<Pair<String, Integer>> getColumns(String schema, String table) throws SQLException {
    DbmsQueryResult queryResult = connection.executeQuery(syntax.getColumnsCommand(schema, table));
    JdbcDriver jdbcQueryResult = new JdbcDriver(queryResult);
    List<Pair<String, Integer>> columns = new ArrayList<>();
    while (queryResult.next()) {
      String type = jdbcQueryResult.getString(syntax.getColumnTypeColumnName());
      // remove the size of type
      type = type.replaceAll("\\(.*\\)", "");
      columns.add(new ImmutablePair<>(jdbcQueryResult.getString(syntax.getColumnNameColumnName()),
          DataTypeConverter.typeInt(type)));
    }
    columnsCache.put(new ImmutablePair<>(schema, table), columns);
    return columns;
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
    DbmsQueryResult queryResult = connection.executeQuery(syntax.getPartitionCommand(schema, table));
    JdbcDriver jdbcQueryResult = new JdbcDriver(queryResult);
    List<String> partition = new ArrayList<>();
    while (queryResult.next()) {
      partition.add(jdbcQueryResult.getString(0));
    }
    partitionCache.addAll(partition);
    return partition;
  }

  public HashMap<Pair<String, String>, List<Pair<String, Integer>>> getColumnsCache() {
    return columnsCache;
  }

  public HashMap<String, List<String>> getTablesCache() {
    return tablesCache;
  }

  public List<String> getSchemaCache() {
    return schemaCache;
  }

  public List<String> getPartitionCache() {
    return partitionCache;
  }
}
