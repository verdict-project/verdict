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

import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.*;

import java.sql.*;
import java.util.*;

public class JdbcConnection implements DbmsConnection {

  Connection conn;

  SqlSyntax syntax;

  String currentSchema = null;

  JdbcQueryResult jrs = null;

  private boolean outputDebugMessage = false;

  private VerdictDBLogger log;

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
//      if (syntax instanceof PostgresqlSyntax || syntax instanceof RedshiftSyntax) {
//        
//      } else {
//        this.currentSchema = conn.getCatalog();
//      }
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
  public void close() {
    try {
      this.conn.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Override
  public DbmsQueryResult execute(String sql) throws VerdictDBDbmsException {

    String quoteChars = "'\"";
    List<String> sqls = splitOnSemicolon(sql, quoteChars);
    DbmsQueryResult finalResult = null;
    for (String s : sqls) {
      finalResult = executeSingle(s);
    }
    return finalResult;
  }

  /**
   * Splits a given query using the delimiter. The delimiters in quote chars are ignored.
   *
   * <p>Note: I have tried many regex-based and the Apache commons library for this, but they do not
   * work. Regex throws StackOverflowError, and the StringTokenizer by the commons library is
   * incorrect for our purpose.
   *
   * @param sql
   */
  private List<String> splitOnSemicolon(String sql, String quoteChars) {
    List<String> splitted = new ArrayList<>();
    Map<Character, Integer> quoteCharCounts = new HashMap<>();
    Set<Character> quoteCharSet = Sets.newHashSet(ArrayUtils.toObject(quoteChars.toCharArray()));
    for (char c : quoteCharSet) {
      quoteCharCounts.put(c, 0);
    }
    char delimiter = ';';

    StringBuilder beginConstructed = new StringBuilder();
    for (char c : sql.toCharArray()) {
      // when encountered a delimiter
      if (c == delimiter) {
        // if there is no odd-count quote chars, we create a new sql
        boolean oddCountQuoteExist = false;
        for (int count : quoteCharCounts.values()) {
          if (count % 2 == 1) {
            oddCountQuoteExist = true;
            break;
          }
        }
        if (oddCountQuoteExist == false) {
          // create a new sql
          splitted.add(beginConstructed.toString());
          beginConstructed = new StringBuilder();
          ;
        }
      } else {
        beginConstructed.append(c);
        if (quoteCharSet.contains(c)) {
          quoteCharCounts.put(c, quoteCharCounts.get(c) + 1);
        }
      }
    }
    // if there anything remaining, add it as a separate sql
    if (beginConstructed.length() > 0) {
      String s = beginConstructed.toString();
      if (s.trim().length() > 0) {
        splitted.add(s);
      }
    }

    return splitted;
  }

  public DbmsQueryResult executeSingle(String sql) throws VerdictDBDbmsException {

    VerdictDBLogger logger = VerdictDBLogger.getLogger(this.getClass());
    logger.debug("Issuing the following query to DBMS: " + sql);

    try {
      Statement stmt = conn.createStatement();
      JdbcQueryResult jrs = null;
      boolean doesResultExist = stmt.execute(sql);
      if (doesResultExist) {
        ResultSet rs = stmt.getResultSet();
        jrs = new JdbcQueryResult(rs);
        rs.close();
      } else {
        jrs = null;
      }
      stmt.close();
      return jrs;
    } catch (SQLException e) {
      //      e.printStackTrace();
      //      logger.debug(StackTraceReader.stackTrace2String(e));
      throw new VerdictDBDbmsException(e.getMessage());
    }
  }

  //  @Override
  //  public DbmsQueryResult getResult() {
  //    return jrs;
  //  }

  public DbmsQueryResult executeQuery(String sql) throws VerdictDBDbmsException {
    return execute(sql);
  }

  //  @Override
  //  public DbmsQueryResult executeQuery(String query) throws VerdictDBDbmsException {
  //    System.out.println("About to issue this query: " + query);
  //    try {
  //      Statement stmt = conn.createStatement();
  //      ResultSet rs = stmt.executeQuery(query);
  //      JdbcQueryResult jrs = new JdbcQueryResult(rs);
  //      rs.close();
  //      stmt.close();
  //      return jrs;
  //    } catch (SQLException e) {
  //      throw new VerdictDBDbmsException(e.getMessage());
  //    }
  //  }
  //
  //  @Override
  //  public int executeUpdate(String query) throws VerdictDBDbmsException {
  //    System.out.println("About to issue this query: " + query);
  //    try {
  //      Statement stmt = conn.createStatement();
  //      int r = stmt.executeUpdate(query);
  //      stmt.close();
  //      return r;
  //    } catch (SQLException e) {
  //      throw new VerdictDBDbmsException(e);
  ////      e.printStackTrace();
  ////      return 0;
  //    }
  //  }

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
  public List<String> getTables(String schema) throws VerdictDBDbmsException {
    List<String> tables = new ArrayList<>();
    DbmsQueryResult queryResult = executeQuery(syntax.getTableCommand(schema));

    while (queryResult.next()) {
      tables.add(queryResult.getString(syntax.getTableNameColumnIndex()));
    }

    return tables;
  }

  @Override
  public List<Pair<String, String>> getColumns(String schema, String table)
      throws VerdictDBDbmsException {
    List<Pair<String, String>> columns = new ArrayList<>();
    DbmsQueryResult queryResult = executeQuery(syntax.getColumnsCommand(schema, table));

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

      //        // remove the size of type
      //        type = type.replaceAll("\\(.*\\)", "");

      columns.add(
          new ImmutablePair<>(queryResult.getString(syntax.getColumnNameColumnIndex()), type));
    }

    return columns;
  }

  @Override
  public List<String> getPartitionColumns(String schema, String table)
      throws VerdictDBDbmsException {
    List<String> partition = new ArrayList<>();
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
