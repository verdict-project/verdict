/**
 * Test the functionality of VerdictSingleResult.print()
 */

package org.verdictdb;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.MysqlSyntax;

import java.sql.*;

public class VerdictSingleResultPrintTest {

  private static final String MYSQL_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_DATABASE =
      "verdictstatement_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "zhongshucheng123";

  private static Statement stmt;

  private static VerdictContext vc;

  @BeforeClass
  public static void setupMySQLdatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    Connection conn =
        DatabaseConnectionHelpers.setupMySql(
            mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD, MYSQL_DATABASE);
    DbmsConnection dbmsConnection = new JdbcConnection(conn, new MysqlSyntax());
    vc = new VerdictContext(dbmsConnection);
    stmt = conn.createStatement();
    vc.sql(String.format(
        "create scramble if not exists %s.lineitem_scrambled from %s.lineitem blocksize 100",
        MYSQL_DATABASE, MYSQL_DATABASE));
    vc.sql(String.format(
        "create scramble if not exists %s.orders_scrambled from %s.orders blocksize 100",
        MYSQL_DATABASE, MYSQL_DATABASE));
  }

  @Test
  public void testPrint() throws VerdictDBException {
    VerdictResultStream verdictResultStream = vc.streamsql(
        String.format("select     l_returnflag,\n" +
            "    l_linestatus,\n" +
            "    sum(l_quantity) as sum_qty from %s.lineitem group by l_linestatus, l_returnflag", MYSQL_DATABASE));
    while (verdictResultStream.hasNext()){
      VerdictSingleResult verdictSingleResult = verdictResultStream.next();
      verdictSingleResult.print();
    }
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute(String.format("drop schema %s", MYSQL_DATABASE));
  }
}
