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
    DbmsQueryResult queryResult = connection.executeQuery(syntax.getSchemaCommand());
    JdbcResultSet jdbcQueryResult = new JdbcResultSet(queryResult);
    while (queryResult.next()) {
      schemaCache.add(jdbcQueryResult.getString(syntax.getSchemaNameColumnIndex()));
    }
    return schemaCache;
  }
  
  public List<String> getTables(String schema) throws SQLException {
    if (!tablesCache.isEmpty()){
      return tablesCache;
    }
    DbmsQueryResult queryResult = connection.executeQuery(syntax.getTableCommand(schema));
    JdbcResultSet jdbcQueryResult = new JdbcResultSet(queryResult);
    while (queryResult.next()) {
      tablesCache.add(jdbcQueryResult.getString(syntax.getTableNameColumnIndex()));
    }
    return tablesCache;
  }
  
  public List<Pair<String, Integer>> getColumns(String schema, String table) throws SQLException {
    if (!columnsCache.isEmpty()){
      return columnsCache;
    }
    DbmsQueryResult queryResult = connection.executeQuery(syntax.getColumnsCommand(schema, table));
    JdbcResultSet jdbcQueryResult = new JdbcResultSet(queryResult);
    while (queryResult.next()) {
      String type = jdbcQueryResult.getString(syntax.getColumnTypeColumnIndex());
      // remove the size of type
      type = type.replaceAll("\\(.*\\)", "");
      columnsCache.add(new ImmutablePair<>(jdbcQueryResult.getString(syntax.getColumnNameColumnIndex()),
          DataTypeConverter.typeInt(type)));
    }
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
    DbmsQueryResult queryResult = connection.executeQuery(syntax.getPartitionCommand(schema, table));
    JdbcResultSet jdbcQueryResult = new JdbcResultSet(queryResult);
    while (queryResult.next()) {
      partitionCache.add(jdbcQueryResult.getString(0));
    }
    return partitionCache;
  }

}
