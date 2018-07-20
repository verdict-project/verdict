package org.verdictdb.sqlreader;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.coordinator.ScramblingCoordinator;
import org.verdictdb.coordinator.SelectQueryCoordinator;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.MysqlSyntax;

public class MySqlSqlToRelationFailureTest {

  // lineitem has 10 blocks, orders has 3 blocks;
  // lineitem join orders has 12 blocks
  final static int blockSize = 100;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static Connection conn;

  private static Statement stmt;

  private static final String MYSQL_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_DATABASE = "coordinator_test";

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "";

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    conn = DatabaseConnectionHelpers.setupMySql(
        mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD, MYSQL_DATABASE);
    stmt = conn.createStatement();
    DbmsConnection dbmsConn = JdbcConnection.create(conn);

    // Create Scramble table
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`lineitem_scrambled`", MYSQL_DATABASE));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`orders_scrambled`", MYSQL_DATABASE));

    ScramblingCoordinator scrambler =
        new ScramblingCoordinator(dbmsConn, MYSQL_DATABASE, MYSQL_DATABASE, (long) 100);
    ScrambleMeta meta1 =
        scrambler.scramble(MYSQL_DATABASE, "lineitem", MYSQL_DATABASE, "lineitem_scrambled", "uniform");
    ScrambleMeta meta2 =
        scrambler.scramble(MYSQL_DATABASE, "orders", MYSQL_DATABASE, "orders_scrambled", "uniform");
    meta.addScrambleMeta(meta1);
    meta.addScrambleMeta(meta2);
  }

  @Test
  public void FailedParserTest1() throws SQLException, VerdictDBException {
    String errorSql = "select\n" +
        "  s_name,\n" +
        "  count(s_address)\n" +
        "fromfrom\n" +    // error syntax
        "  supplier,\n" +
        "  nation,\n" +
        "  partsupp,\n" +
        "  (select\n" +
        "    l_partkey,\n" +
        "    l_suppkey,\n" +
        "    0.5 * sum(l_quantity) as sum_quantity\n" +
        "  from\n" +
        "    lineitem_scrambled\n" +
        "where\n" +
        "  l_shipdate >= '1994-01-01'\n" +
        "  and l_shipdate < '1998-01-01'\n" +
        "group by l_partkey, l_suppkey) as q20_tmp2_cached\n" +
        "where\n" +
        "  s_nationkey = n_nationkey\n" +
        "  and n_name = 'CANADA'\n" +
        "  and s_suppkey = ps_suppkey\n" +
        "  group by s_name\n" +
        "order by s_name";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    DbmsConnection dbmsconn = new CachedDbmsConnection(
        new JdbcConnection(conn, new MysqlSyntax()));
    dbmsconn.setDefaultSchema(MYSQL_DATABASE);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);
    coordinator.setScrambleMetaSet(meta);
    try {
      coordinator.process(errorSql);
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("syntax error occurred:extraneous input "));
    }
  }

  @Test
  public void FailedParserTest2() throws SQLException, VerdictDBException {
    String errorSql = "select\n" +
        "  s_name,\n" +
        "  count((s_address)\n" + // error syntax
        "from\n" +
        "  supplier,\n" +
        "  nation,\n" +
        "  partsupp,\n" +
        "  (select\n" +
        "    l_partkey,\n" +
        "    l_suppkey,\n" +
        "    0.5 * sum(l_quantity) as sum_quantity\n" +
        "  from\n" +
        "    lineitem_scrambled\n" +
        "where\n" +
        "  l_shipdate >= '1994-01-01'\n" +
        "  and l_shipdate < '1998-01-01'\n" +
        "group by l_partkey, l_suppkey) as q20_tmp2_cached\n" +
        "where\n" +
        "  s_nationkey = n_nationkey\n" +
        "  and n_name = 'CANADA'\n" +
        "  and s_suppkey = ps_suppkey\n" +
        "  group by s_name\n" +
        "order by s_name";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    DbmsConnection dbmsconn = new CachedDbmsConnection(
        new JdbcConnection(conn, new MysqlSyntax()));
    dbmsconn.setDefaultSchema(MYSQL_DATABASE);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);
    coordinator.setScrambleMetaSet(meta);
    try {
      coordinator.process(errorSql);
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().startsWith("syntax error occurred:mismatched input"));
    }
  }
}
