package org.verdictdb.connection;

import static java.sql.Types.CHAR;
import static java.sql.Types.VARCHAR;

import org.verdictdb.commons.DataTypeConverter;
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

  private static final String DB_CONNECTION = "jdbc:h2:mem:verdictdb;DB_CLOSE_DELAY=-1";

  private static final String selectAsyncAggTable = "VERDICTDB_SELECTASYNCAGG";

  private static long selectAsyncAggTableID = 0;

  private static SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new H2Syntax());

  static Connection conn;

  static {
    try {
      Class.forName("org.h2.Driver");
      conn = DriverManager.getConnection(DB_CONNECTION, "", "");
    } catch (SQLException | ClassNotFoundException e) {
      // https://stackoverflow.com/questions/2070293/why-doesnt-java-allow-to-throw-a-checked-exception-from-static-initialization-b
      throw new ExceptionInInitializerError(e);
    }
  }

  public static void createTable(DbmsQueryResult dbmsQueryResult, String tableName) throws SQLException {
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

  public static DbmsQueryResult executeQuery(SelectQuery query) throws VerdictDBException, SQLException {
    String sql = selectQueryToSql.toSql(query).toUpperCase();
    ResultSet rs = conn.createStatement().executeQuery(sql);
    return new JdbcQueryResult(rs);
  }

  public static String combinedTableName(String combineTableName, String targetTableName, SelectQuery dependentQuery)
      throws SQLException, VerdictDBException {
    String tableName = selectAsyncAggTable + selectAsyncAggTableID++;

    // check targetTable exists
    if (targetTableName.equals("")) {
      // if not just let it be the copy of combineTable
      conn.createStatement().execute(
          String.format("CREATE TABLE %s AS SELECT * FROM %s", tableName, combineTableName));
    } else {
      // if exists, combinedTableName two tables using the logic of AggCombinerExecutionNode
      List<GroupingAttribute> groupList = new ArrayList<>();
      SelectQuery copy = dependentQuery.deepcopy();
      for (SelectItem sel : copy.getSelectList()) {
        if (sel instanceof AliasedColumn) {
          UnnamedColumn col = ((AliasedColumn) sel).getColumn();
          resetSchemaAndTableForCombine(col);
          String alias = ((AliasedColumn) sel).getAliasName().toUpperCase();
          ((AliasedColumn) sel).setAliasName(alias);
          if (col.isAggregateColumn()) {
            if (col instanceof ColumnOp && ((ColumnOp) col).getOpType().equals("max")) {
              ((AliasedColumn) sel).setColumn(new ColumnOp("max", new BaseColumn(alias)));
            } else if (col instanceof ColumnOp && ((ColumnOp) col).getOpType().equals("min")) {
              ((AliasedColumn) sel).setColumn(new ColumnOp("min", new BaseColumn(alias)));
            } else {
              ((AliasedColumn) sel).setColumn(new ColumnOp("sum", new BaseColumn(alias)));
            }
          } else {
            ((AliasedColumn) sel).setColumn(new BaseColumn(alias));
            groupList.add(((AliasedColumn) sel).getColumn());
          }
        }
      }
      SelectQuery left = SelectQuery.create(new AsteriskColumn(),
          new BaseTable("PUBLIC", targetTableName));
      SelectQuery right = SelectQuery.create(new AsteriskColumn(),
          new BaseTable("PUBLIC", combineTableName));
      AbstractRelation setOperation = new SetOperationRelation(left, right, SetOperationRelation.SetOpType.unionAll);
      copy.clearFilter();
      copy.setFromList(Arrays.asList(setOperation));
      copy.clearGroupby();
      copy.addGroupby(groupList);
      String sql = selectQueryToSql.toSql(copy);
      conn.createStatement().execute(
          String.format("CREATE TABLE %s AS %s", tableName, sql));
    }

    return tableName;
  }

  private static void resetSchemaAndTableForCombine(UnnamedColumn column) {
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

}
