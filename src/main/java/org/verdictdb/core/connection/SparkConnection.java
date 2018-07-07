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
    while (queryResult.next()) {
      schemas.add((String) queryResult.getValue(syntax.getSchemaNameColumnIndex()));
    }
    return schemas;
  }

  @Override
  public List<String> getTables(String schema) throws VerdictDBDbmsException {
    List<String> tables = new ArrayList<>();
    DbmsQueryResult queryResult = execute(syntax.getTableCommand(schema));
    while (queryResult.next()) {
      tables.add((String) queryResult.getValue(syntax.getTableNameColumnIndex()));
    }
    return tables;
  }

  @Override
  public List<Pair<String, String>> getColumns(String schema, String table) throws VerdictDBDbmsException {
    List<Pair<String, String>> columns = new ArrayList<>();
    DbmsQueryResult queryResult = execute(syntax.getColumnsCommand(schema, table));
    while (queryResult.next()) {
      String type = (String) queryResult.getValue(syntax.getColumnTypeColumnIndex());
      type = type.toLowerCase();

      columns.add(
          new ImmutablePair<>((String) queryResult.getValue(syntax.getColumnNameColumnIndex()), type));
    }

    return columns;
  }

  @Override
  public List<String> getPartitionColumns(String schema, String table) throws VerdictDBDbmsException {
    List<String> partition = new ArrayList<>();
    DbmsQueryResult queryResult = execute(syntax.getPartitionCommand(schema, table));
    while (queryResult.next()) {
      partition.add((String) queryResult.getValue(0));
    }

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
      if (result != null) {
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
