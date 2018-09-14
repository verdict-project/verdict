/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.connection;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.StringSplitter;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.HiveSyntax;
import org.verdictdb.sqlsyntax.ImpalaSyntax;
import org.verdictdb.sqlsyntax.PostgresqlSyntax;
import org.verdictdb.sqlsyntax.RedshiftSyntax;
import org.verdictdb.sqlsyntax.SparkSyntax;
import org.verdictdb.sqlsyntax.SqlSyntax;
import org.verdictdb.sqlsyntax.SqlSyntaxList;

public class JdbcConnection extends DbmsConnection {

  Connection conn;

  SqlSyntax syntax;

  String currentSchema = null;

  JdbcQueryResult jrs = null;

  private boolean outputDebugMessage = false;
  
  private Statement runningStatement = null;

  private VerdictDBLogger log;
  
  private boolean isAborting = false;

  public static JdbcConnection create(Connection conn) throws VerdictDBDbmsException {
    String connectionString = null;
    try {
      connectionString = conn.getMetaData().getURL();
    } catch (SQLException e) {
      throw new VerdictDBDbmsException(e);
    }

    SqlSyntax syntax = SqlSyntaxList.getSyntaxFromConnectionString(connectionString);
    //    String dbName = connectionString.split(":")[1];
    //    SqlSyntax syntax = SqlSyntaxList.getSyntaxFor(dbName);

    JdbcConnection jdbcConn = new JdbcConnection(conn, syntax);
    //    jdbcConn.setOutputDebugMessage(true);
    return jdbcConn;
  }

  public JdbcConnection(Connection conn, SqlSyntax syntax) {
    this.conn = conn;
    try {
      this.currentSchema = conn.getSchema();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    // set a default value if an inappropriate value is set.
    if (currentSchema == null || currentSchema.length() == 0) {
      currentSchema = syntax.getFallbackDefaultSchema();
    }

    this.syntax = syntax;
    this.log = VerdictDBLogger.getLogger(this.getClass());
  }
  
  @Override
  public void abort() {
    log.trace("Aborts a statement if running.");
    isAborting = true;
    try {
      synchronized (this) {
        if (runningStatement != null && !runningStatement.isClosed()) {
          log.trace("Aborts a running statement.");
          runningStatement.cancel();
          runningStatement.close();
          runningStatement = null;
        }
      }
      
      isAborting = false;
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    log.debug("Closes a JDBC connection.");
    abort();
    try {
      this.conn.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public DbmsQueryResult execute(String sql) throws VerdictDBDbmsException {
    String quoteChars = "'\"";
    List<String> sqls = StringSplitter.splitOnSemicolon(sql, quoteChars);
    DbmsQueryResult finalResult = null;
    for (String s : sqls) {
      finalResult = executeSingle(s);
    }
    return finalResult;
  }

  
  
  private void setRunningStatement(Statement stmt) {
    synchronized (this) {
      runningStatement = stmt;
    }
  }
  
  private Statement getRunningStatement() {
    synchronized (this) {
      return runningStatement;
    }
  }

  public DbmsQueryResult executeSingle(String sql) throws VerdictDBDbmsException {
    log.debug("Issuing the following query to DBMS: " + sql);
    System.out.println("Issuing the following query to DBMS: " + sql);
    try {
      Statement stmt = conn.createStatement();
      setRunningStatement(stmt);
      JdbcQueryResult jrs = null;
      boolean doesResultExist = stmt.execute(sql);
      if (doesResultExist) {
        ResultSet rs = stmt.getResultSet();
        jrs = new JdbcQueryResult(rs);
        rs.close();
      } else {
        jrs = null;
      }
      setRunningStatement(null);
      stmt.close();
      return jrs;
    } catch (SQLException e) {
      if (isAborting) {
        return null;
      } else {
        String msg = "Issued the following query: " + sql + "\n" + e.getMessage();
        throw new VerdictDBDbmsException(msg);
      }
    }
  }

  public DbmsQueryResult executeQuery(String sql) throws VerdictDBDbmsException {
    return execute(sql);
  }

  @Override
  public SqlSyntax getSyntax() {
    return syntax;
  }

  public Connection getConnection() {
    return conn;
  }

  @Override
  public List<String> getSchemas() throws VerdictDBDbmsException {
    List<String> schemas = new ArrayList<>();
    DbmsQueryResult queryResult = executeQuery(syntax.getSchemaCommand());

    while (queryResult.next()) {
      schemas.add(queryResult.getString(syntax.getSchemaNameColumnIndex()));
    }

    return schemas;
  }

  @Override
  public List<String> getTables(String schema) {
    List<String> tables = new ArrayList<>();
    try {
      DbmsQueryResult queryResult = executeQuery(syntax.getTableCommand(schema));
      while (queryResult.next()) {
        tables.add(queryResult.getString(syntax.getTableNameColumnIndex()));
      }
    } catch (VerdictDBDbmsException e) {
      log.debug(e.getMessage());
    }
    return tables;
  }

  @Override
  public List<Pair<String, String>> getColumns(String schema, String table)
      throws VerdictDBDbmsException {
    List<Pair<String, String>> columns = new ArrayList<>();
    String sql = syntax.getColumnsCommand(schema, table);
    
    try {
      DbmsQueryResult queryResult = executeQuery(sql);
      while (queryResult.next()) {
        String type;
        if (syntax instanceof PostgresqlSyntax) {
          type = queryResult.getString(syntax.getColumnTypeColumnIndex());
          if (queryResult.getInt(((PostgresqlSyntax) syntax).getCharacterMaximumLengthColumnIndex())
              != 0) {
            type =
                type
                    + "("
                    + queryResult.getInt(
                        ((PostgresqlSyntax) syntax).getCharacterMaximumLengthColumnIndex())
                    + ")";
          }
        } else {
          type = queryResult.getString(syntax.getColumnTypeColumnIndex());
        }
        type = type.toLowerCase();

        columns.add(
            new ImmutablePair<>(queryResult.getString(syntax.getColumnNameColumnIndex()), type));
      }
      
    } catch (Exception e) {
      if (syntax instanceof RedshiftSyntax && e.getMessage().matches("(?s).*schema .* does not exist;.*")) {
         return columns;
      } else {
        throw e;
      }
    }

    return columns;
  }

  @Override
  public List<String> getPartitionColumns(String schema, String table)
      throws VerdictDBDbmsException {
    List<String> partition = new ArrayList<>();
    
    if (!syntax.doesSupportTablePartitioning()) {
      return partition;
    }
    
    DbmsQueryResult queryResult;
    if (syntax instanceof ImpalaSyntax) {
      try {
        queryResult = executeQuery(syntax.getPartitionCommand(schema, table));
        for (int i = 0; i < queryResult.getColumnCount(); i++) {
          String columnName = queryResult.getColumnName(i);
          if (columnName.equals("#rows")) {
            break;
          } else partition.add(columnName);
        }
        return partition;
      } catch (Exception e) {
        if (e.getMessage().contains("Table is not partitioned")) {
          return partition;
        } else {
          throw e;
        }
      }
    } else if (syntax instanceof RedshiftSyntax) {
      try {
        queryResult = executeQuery(syntax.getPartitionCommand(schema, table));
      } catch (Exception e) {
        if (e.getMessage().matches("(?s).*schema .* does not exist;.*")) {
          return partition;
        } else {
          throw e;
        }
      }
    } else {
      queryResult = executeQuery(syntax.getPartitionCommand(schema, table));
    }

    // the result of postgresql is a vector of column index
    if (syntax instanceof PostgresqlSyntax) {
      if (queryResult.next()) {
        Object o = queryResult.getValue(0);
        String[] arr = o.toString().split(" ");
        List<Pair<String, String>> columns = getColumns(schema, table);
        for (int i = 0; i < arr.length; i++) {
          partition.add(columns.get(Integer.valueOf(arr[i]) - 1).getKey());
        }
      }
    }
    // Hive and Spark append partition information at the end of the "DESCRIBE TABLE" statement.
    else if (syntax instanceof HiveSyntax || syntax instanceof SparkSyntax) {
      boolean hasPartitionInfoStarted = false;
      while (queryResult.next()) {
        String name = queryResult.getString(0);
        if (hasPartitionInfoStarted && (name.equalsIgnoreCase("# col_name") == false)) {
          partition.add(name);
        } else if (name.equalsIgnoreCase("# Partition Information")) {
          hasPartitionInfoStarted = true;
        }
      }
    } else {
      while (queryResult.next()) {
        partition.add(queryResult.getString(0));
      }
    }

    return partition;
  }

  @Override
  public String getDefaultSchema() {
    return currentSchema;
  }

  @Override
  public void setDefaultSchema(String schema) {
    currentSchema = schema;
  }

  public DatabaseMetaData getMetadata() throws VerdictDBDbmsException {
    try {
      return conn.getMetaData();
    } catch (SQLException e) {
      throw new VerdictDBDbmsException(e);
    }
  }

  public boolean isOutputDebugMessage() {
    return outputDebugMessage;
  }

  public void setOutputDebugMessage(boolean outputDebugMessage) {
    this.outputDebugMessage = outputDebugMessage;
  }

  @Override
  public DbmsConnection copy() {
    JdbcConnection newConn = new JdbcConnection(conn, syntax);
    newConn.setDefaultSchema(currentSchema);
    newConn.jrs = this.jrs;
    newConn.outputDebugMessage = this.outputDebugMessage;
    return newConn;
  }
}
