package org.verdictdb.core.connection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.jdbc41.JdbcResultSet;
import org.verdictdb.sqlsyntax.PostgresqlSyntax;
import org.verdictdb.sqlsyntax.SparkSyntax;
import org.verdictdb.sqlsyntax.SqlSyntax;

public class SparkConnection implements DbmsConnection {

  SparkSession sc;

  SqlSyntax syntax;
  
  public SparkConnection(SparkSession sc, SqlSyntax syntax) {
    this.sc = sc;
    this.syntax = syntax;
  }

  @Override
  public List<String> getSchemas() throws VerdictDBDbmsException {
    List<String> schemas = new ArrayList<>();
    DbmsQueryResult queryResult = execute(syntax.getSchemaCommand());
    JdbcResultSet jdbcResultSet = new JdbcResultSet(queryResult);
    try {
      while (queryResult.next()) {
        schemas.add(jdbcResultSet.getString(syntax.getSchemaNameColumnIndex()+1));
      }
    } catch (SQLException e) {
      throw new VerdictDBDbmsException(e);
    } finally {
      jdbcResultSet.close();
    }
    return schemas;
  }

  @Override
  public List<String> getTables(String schema) throws VerdictDBDbmsException {
    List<String> tables = new ArrayList<>();
    DbmsQueryResult queryResult = execute(syntax.getTableCommand(schema));
    JdbcResultSet jdbcResultSet = new JdbcResultSet(queryResult);
    try {
      while (queryResult.next()) {
        tables.add(jdbcResultSet.getString(syntax.getTableNameColumnIndex()+1));
      }
    } catch (SQLException e) {
      throw new VerdictDBDbmsException(e);
    } finally {
      jdbcResultSet.close();
    }
    return tables;
  }

  @Override
  public List<Pair<String, String>> getColumns(String schema, String table) throws VerdictDBDbmsException {
    List<Pair<String, String>> columns = new ArrayList<>();
    DbmsQueryResult queryResult = execute(syntax.getColumnsCommand(schema, table));
    JdbcResultSet jdbcResultSet = new JdbcResultSet(queryResult);
    try {
      while (queryResult.next()) {
        String type = jdbcResultSet.getString(syntax.getColumnTypeColumnIndex()+1);
        type = type.toLowerCase();

//        // remove the size of type
//        type = type.replaceAll("\\(.*\\)", "");

        columns.add(
            new ImmutablePair<>(jdbcResultSet.getString(syntax.getColumnNameColumnIndex()+1), type));
      }
    } catch (SQLException e) {
      throw new VerdictDBDbmsException(e);
    } finally {
      jdbcResultSet.close();
    }
    return columns;
  }

  @Override
  public List<String> getPartitionColumns(String schema, String table) throws VerdictDBDbmsException {
    List<String> partition = new ArrayList<>();
    DbmsQueryResult queryResult = execute(syntax.getPartitionCommand(schema, table));
    JdbcResultSet jdbcResultSet = new JdbcResultSet(queryResult);
    // the result of postgresql is a vector of column index

    try {
      if (syntax instanceof PostgresqlSyntax) {
        queryResult.next();
        Object o = jdbcResultSet.getObject(1);
        String[] arr = o.toString().split(" ");
        List<Pair<String, String>> columns = getColumns(schema, table);
        for (int i=0; i<arr.length; i++) {
          partition.add(columns.get(Integer.valueOf(arr[i])-1).getKey());
        }
      }
      else {
        while (queryResult.next()) {
          partition.add(jdbcResultSet.getString(1));
        }
      }
    } catch (SQLException e) {
      throw new VerdictDBDbmsException(e);
    } finally {
      jdbcResultSet.close();
    }
    jdbcResultSet.close();
    return partition;
  }

  @Override
  public String getDefaultSchema() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public DbmsQueryResult execute(String query) throws VerdictDBDbmsException {

    try {
      SparkQueryResult srs = null;
      Dataset<Row> result = sc.sql(query);
      if (result!=null) {
        srs = new SparkQueryResult(result);
      }
      return srs;
    } catch (Exception e) {
      throw new VerdictDBDbmsException(e.getMessage());
    }
  }

  @Override
  public SqlSyntax getSyntax() {
    return syntax;
  }

  @Override
  public Connection getConnection() {
    return null;
  }

  @Override
  public void close() {
    try {
      this.sc.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public SparkSession getSparkSession() {
    return sc;
  }

}
