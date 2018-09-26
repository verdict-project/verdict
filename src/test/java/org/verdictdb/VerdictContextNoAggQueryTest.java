package org.verdictdb;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlwriter.SelectQueryToSql;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class VerdictContextNoAggQueryTest {

  static Connection conn;

  static DbmsConnection dbmsConnection;

  static Statement stmt;

  private static final String MYSQL_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_DATABASE = "test";

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "";

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBException {
    // TPCH
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    conn =
        DatabaseConnectionHelpers.setupMySql(
            mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD, MYSQL_DATABASE);
    dbmsConnection = JdbcConnection.create(conn);
    dbmsConnection.setDefaultSchema(MYSQL_DATABASE);
    stmt = conn.createStatement();
    stmt.execute(
        String.format(
            "create schema if not exists `%s`", VerdictOption.getDefaultTempSchemaName()));

    // Table with a confusing column name
    conn.createStatement()
        .execute(
            String.format(
                "create table `%s`.`simpletable` (age integer, height float)", MYSQL_DATABASE));
  }

  @Test
  public void orderByConflictingNames() throws VerdictDBException {
    String sql =
        String.format("select * from `%s`.`simpletable` order by age, height", MYSQL_DATABASE);
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    verdictContext.sql(sql);
  }

  @Test
  public void BasicSelectTest() throws VerdictDBException, IOException {
    File schemaFile = new File("src/test/resources/noAggQuery/basicSelectQuery.sql");
    String sql = Files.toString(schemaFile, Charsets.UTF_8);
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    VerdictSingleResult result = verdictContext.sql(sql);
    while (result.next()) {
      assertEquals(1, result.getRowCount());
      assertEquals(3, result.getInt(0));
    }
  }

  @Test
  public void SimpleSelectTest() throws VerdictDBException, SQLException, IOException {
    File schemaFile = new File("src/test/resources/noAggQuery/simpleSelectQuery.sql");
    String sql = Files.toString(schemaFile, Charsets.UTF_8);
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    VerdictSingleResult result = verdictContext.sql(sql);
    ResultSet rs = stmt.executeQuery(sql);
    while (result.next()) {
      rs.next();
      assertEquals(rs.getInt(1), result.getInt(0));
    }
  }

  @Test
  public void SimpleAggTest() throws VerdictDBException, SQLException, IOException {
    File schemaFile = new File("src/test/resources/noAggQuery/simpleAggQuery.sql");
    String sql = Files.toString(schemaFile, Charsets.UTF_8);
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    VerdictSingleResult result = verdictContext.sql(sql);
    ResultSet rs = stmt.executeQuery(sql);
    while (result.next()) {
      rs.next();
      assertEquals(rs.getInt(1), result.getInt(0));
    }
  }

  /*
  Wrong without simplify
   */
  // @Test
  public void TpchQuery2Test() throws VerdictDBException, SQLException, IOException {
    File schemaFile = new File("src/test/resources/noAggQuery/tpchQuery2.sql");
    String sql = Files.toString(schemaFile, Charsets.UTF_8);
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    VerdictSingleResult result = verdictContext.sql(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConnection);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    ResultSet rs = stmt.executeQuery(stdQuery);
    while (result.next()) {
      rs.next();
      assertEquals(result.getDouble(0), rs.getDouble(1), 1e-5);
      assertEquals(result.getString(1), rs.getString(2));
      assertEquals(result.getString(2), rs.getString(3));
      assertEquals(result.getInt(3), rs.getInt(4));
      assertEquals(result.getString(4), rs.getString(5));
      assertEquals(result.getString(5), rs.getString(6));
      assertEquals(result.getString(6), rs.getString(7));
      assertEquals(result.getString(7), rs.getString(8));
    }
  }
}
