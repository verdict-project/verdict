package org.verdictdb.connection;

import static java.sql.Types.CHAR;
import static java.sql.Types.VARCHAR;

import org.apache.commons.lang3.RandomStringUtils;
import org.verdictdb.commons.DataTypeConverter;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.H2Syntax;
import org.verdictdb.sqlwriter.SelectQueryToSql;

import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InMemoryAggregate {

  private static final String selectAsyncAggTable = "VERDICTDB_SELECTASYNCAGG";

  private long selectAsyncAggTableID = 0;

  private static SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new H2Syntax());

  private Connection conn;
  
  private boolean aborted = false;
  
  private VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());;

  public static InMemoryAggregate create() {
    InMemoryAggregate inMemoryAggregate = null;
    try {
      Class.forName("org.h2.Driver");
      inMemoryAggregate = new InMemoryAggregate();
      String h2Database =
          "verdictdb_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
      String DB_CONNECTION = String.format("jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1", h2Database);
      inMemoryAggregate.conn = DriverManager.getConnection(DB_CONNECTION, "", "");
    } catch (SQLException | ClassNotFoundException e) {
      // https://stackoverflow.com/questions/2070293/why-doesnt-java-allow-to-throw-a-checked-exception-from-static-initialization-b
      throw new ExceptionInInitializerError(e);
    }
    return inMemoryAggregate;
  }

  public void createTable(DbmsQueryResult dbmsQueryResult, String tableName) throws SQLException {
    // the query processing has already been finished; thus, no more processing will be needed.
    if (aborted) {
      return;
    }
    
    StringBuilder columnNames = new StringBuilder();
    StringBuilder fieldNames = new StringBuilder();
    StringBuilder bindVariables = new StringBuilder();
    for (int i = 0; i < dbmsQueryResult.getColumnCount(); i++) {
      if (i > 0) {
        columnNames.append(", ");
        fieldNames.append(", ");
        bindVariables.append(", ");
      }
      fieldNames.append(dbmsQueryResult.getColumnName(i));
      fieldNames.append(" ");
      // char -> varchar in case this type is an array of char
      int columnType = dbmsQueryResult.getColumnType(i);
      if (columnType == CHAR) {
        columnType = VARCHAR;
      }
      fieldNames.append(DataTypeConverter.typeName(columnType));
      columnNames.append(dbmsQueryResult.getColumnName(i));
      bindVariables.append('?');
    }
    // create table
    String createSql = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + fieldNames + ")";
    Statement stmt = conn.createStatement();
    stmt.execute(createSql);
    stmt.close();

    // insert values
    String sql = "INSERT INTO " + tableName + " ("
        + columnNames
        + ") VALUES ("
        + bindVariables
        + ")";
    PreparedStatement statement = conn.prepareStatement(sql);
    while (dbmsQueryResult.next()) {
      for (int i = 1; i <= dbmsQueryResult.getColumnCount(); i++) {
        statement.setObject(i, dbmsQueryResult.getValue(i - 1));
      }
      statement.addBatch();
    }
    statement.executeBatch();
    statement.close();
  }

  public DbmsQueryResult executeQuery(SelectQuery query) throws VerdictDBException, SQLException {
    // the query processing has already been finished; thus, no more processing will be needed.
    if (aborted) {
      return null;
    }
    
    String sql = selectQueryToSql.toSql(query).toUpperCase();
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(sql);
    DbmsQueryResult dbmsQueryResult = new JdbcQueryResult(rs);
    rs.close();
    stmt.close();
    return dbmsQueryResult;
  }

  public String combineTables(String combinedTableName, String newAggTableName, SelectQuery dependentQuery)
      throws SQLException, VerdictDBException {
    // the query processing has already been finished; thus, no more processing will be needed.
    if (aborted) {
      return null;
    }
    
    String tableName = selectAsyncAggTable + selectAsyncAggTableID++;

    // check targetTable exists
    if (newAggTableName.equals("")) {
      // if not just let it be the copy of combineTable
      Statement stmt = conn.createStatement();
      stmt.execute(
          String.format("CREATE TABLE %s AS SELECT * FROM %s", tableName, combinedTableName));
      stmt.close();
      
    } else {
      // if exists, combineTables two tables using the logic of AggCombinerExecutionNode
      List<GroupingAttribute> groupList = new ArrayList<>();
      SelectQuery copy = dependentQuery.deepcopy();
      
      for (SelectItem sel : copy.getSelectList()) {
        if (sel instanceof AliasedColumn) {
          UnnamedColumn col = ((AliasedColumn) sel).getColumn();
          resetSchemaAndTableForCombining(col);
          String alias = ((AliasedColumn) sel).getAliasName().toUpperCase();
          ((AliasedColumn) sel).setAliasName(alias);
          
          if (col.isAggregateColumn()) {
            if (col instanceof ColumnOp && ((ColumnOp) col).getOpType().equals("max")) {
              ((AliasedColumn) sel).setColumn(new ColumnOp("max", new BaseColumn(alias)));
            } else if (col instanceof ColumnOp && ((ColumnOp) col).getOpType().equals("min")) {
              ((AliasedColumn) sel).setColumn(new ColumnOp("min", new BaseColumn(alias)));
            } else {    // count, sum, countdistinct, approx_countdistinct
              ((AliasedColumn) sel).setColumn(new ColumnOp("sum", new BaseColumn(alias)));
            }
            
          } else {
            ((AliasedColumn) sel).setColumn(new BaseColumn(alias));
            groupList.add(((AliasedColumn) sel).getColumn());
          }
        }
      }
      
      SelectQuery left = SelectQuery.create(new AsteriskColumn(),
          new BaseTable("PUBLIC", newAggTableName));
      SelectQuery right = SelectQuery.create(new AsteriskColumn(),
          new BaseTable("PUBLIC", combinedTableName));
      AbstractRelation setOperation = new SetOperationRelation(left, right, SetOperationRelation.SetOpType.unionAll);
      copy.clearFilter();
      copy.setFromList(Arrays.asList(setOperation));
      copy.clearGroupby();
      copy.addGroupby(groupList);
      String sql = selectQueryToSql.toSql(copy);
      
      log.debug("Issues the following query to an in-memory db: " + sql);
      
      Statement stmt = conn.createStatement();
      stmt.execute(
          String.format("CREATE TABLE %s AS %s", tableName, sql));
      stmt.close();
    }

    return tableName;
  }

  private static void resetSchemaAndTableForCombining(UnnamedColumn column) {
    List<UnnamedColumn> columns = new ArrayList<>();
    columns.add(column);
    while (!columns.isEmpty()) {
      UnnamedColumn col = columns.get(0);
      columns.remove(0);
      if (col instanceof ColumnOp) {
        columns.addAll(((ColumnOp) col).getOperands());
      }
      if (col instanceof BaseColumn) {
        ((BaseColumn) col).setSchemaName("");
        ((BaseColumn) col).setTableName("UNIONTABLE");
        ((BaseColumn) col).setTableSourceAlias("");
        ((BaseColumn) col).setColumnName(((BaseColumn) col).getColumnName().toUpperCase());
      } else if (col instanceof ColumnOp) {
        columns.addAll(((ColumnOp) col).getOperands());
      }
    }
  }

  public void abort() {
    aborted = true;
    
    try {
      if (!conn.isClosed()) {
        // This will close all the connection and the database.
        Statement stmt = conn.createStatement();
        stmt.execute("SHUTDOWN");
        stmt.close();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

}
