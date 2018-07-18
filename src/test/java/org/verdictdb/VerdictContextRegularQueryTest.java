package org.verdictdb;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.VerdictContext;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.coordinator.VerdictSingleResult;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlwriter.SelectQueryToSql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class VerdictContextRegularQueryTest {

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

  private static final String MYSQL_PASSWORD = "zhongshucheng123";

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    conn = DatabaseConnectionHelpers.setupMySql(
        mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD, MYSQL_DATABASE);
    dbmsConnection = JdbcConnection.create(conn);
    dbmsConnection.setDefaultSchema(MYSQL_DATABASE);
    stmt = conn.createStatement();
  }

  @Test
  public void BasicSelectTest() throws VerdictDBException {
    String sql = "select 1+2";
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    VerdictSingleResult result = verdictContext.sql(sql);
    while (result.next()) {
      assertEquals(1, result.getRowCount());
      assertEquals(3, result.getInt(0));
    }
  }

  @Test
  public void SimpleSelectTest() throws VerdictDBException, SQLException {
    String sql = "select r_regionkey from test.region order by r_regionkey";
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    VerdictSingleResult result = verdictContext.sql(sql);
    ResultSet rs = stmt.executeQuery(sql);
    while (result.next()) {
      rs.next();
      assertEquals(rs.getInt(1), result.getInt(0));
    }
  }

  @Test
  public void SimpleAggTest() throws VerdictDBException, SQLException {
    String sql = "select count(*) from test.region";
    VerdictContext verdictContext = new VerdictContext(dbmsConnection);
    VerdictSingleResult result = verdictContext.sql(sql);
    ResultSet rs = stmt.executeQuery(sql);
    while (result.next()) {
      rs.next();
      assertEquals(rs.getInt(1), result.getInt(0));
    }
  }

  @Test
  public void TpchQuery2Test() throws VerdictDBException, SQLException {
    String sql = "select\n" +
        "  s_acctbal,\n" +
        "  s_name,\n" +
        "  n_name,\n" +
        "  p_partkey,\n" +
        "  p_mfgr,\n" +
        "  s_address,\n" +
        "  s_phone,\n" +
        "  s_comment\n" +
        "from\n" +
        "  part,\n" +
        "  supplier,\n" +
        "  partsupp,\n" +
        "  nation,\n" +
        "  region\n" +
        "where\n" +
        "  p_partkey = ps_partkey\n" +
        "  and s_suppkey = ps_suppkey\n" +
        "  and p_size = 37\n" +
        "  and p_type like '%COPPER'\n" +
        "  and s_nationkey = n_nationkey\n" +
        "  and n_regionkey = r_regionkey\n" +
        "  and r_name = 'EUROPE'\n" +
        "  and ps_supplycost = (\n" +
        "    select\n" +
        "      min(ps_supplycost)\n" +
        "    from\n" +
        "      partsupp,\n" +
        "      supplier,\n" +
        "      nation,\n" +
        "      region\n" +
        "    where\n" +
        "      p_partkey = ps_partkey\n" +
        "      and s_suppkey = ps_suppkey\n" +
        "      and s_nationkey = n_nationkey\n" +
        "      and n_regionkey = r_regionkey\n" +
        "      and r_name = 'EUROPE'\n" +
        "  )\n" +
        "order by\n" +
        "  s_acctbal desc,\n" +
        "  n_name,\n" +
        "  s_name,\n" +
        "  p_partkey\n" +
        "limit 100;";
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
