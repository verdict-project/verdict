/**
 * Test the Correctness of VerdictStatement.sql() executing stream query using TPCH queries. It will
 * compare direct query result from Mysql with Verdictdb's results from final block, which is
 * supposed to be the same with the original results.
 */
package org.verdictdb.jdbc41;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.exception.VerdictDBException;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class VerdictStatementStreamSqlTest {

  private static VerdictConnection verdictConn;

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

  private static final String MYSQL_PASSWORD = "";

  private static Statement stmt, vstmt;

  @BeforeClass
  public static void setupMySQLdatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    Connection conn =
        DatabaseConnectionHelpers.setupMySql(
            mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD, MYSQL_DATABASE);
    stmt = conn.createStatement();
    mysqlConnectionString =
        String.format("jdbc:verdict:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    verdictConn =
        (VerdictConnection)
            DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);
    verdictConn
        .createStatement()
        .execute(
            String.format(
                "create scramble if not exists %s.lineitem_scrambled from %s.lineitem blocksize 100",
                MYSQL_DATABASE, MYSQL_DATABASE));
    verdictConn
        .createStatement()
        .execute(
            String.format(
                "create scramble if not exists %s.orders_scrambled from %s.orders blocksize 100",
                MYSQL_DATABASE, MYSQL_DATABASE));
    vstmt = verdictConn.createStatement();
  }

  Pair<ResultSet, ResultSet> getAnswerPair(int queryNum)
      throws VerdictDBException, SQLException, IOException {
    String filename = "query" + queryNum + ".sql";
    File file = new File("src/test/resources/tpch_test_query/" + filename);
    String sql = Files.toString(file, Charsets.UTF_8);

    stmt.execute(String.format("use %s", MYSQL_DATABASE));
    vstmt.execute(String.format("use %s", MYSQL_DATABASE));
    ResultSet rs = stmt.executeQuery(sql);
    if (queryNum >= 100) {
      sql = sql.replaceAll("lineitem", "lineitem_hash_scrambled");
    } else {
      sql = sql.replaceAll("lineitem", "lineitem_scrambled");
    }
    sql = sql.replaceAll("orders", "orders_scrambled");
    ResultSet vrs = ((VerdictStatement) vstmt).executeQuery("STREAM " + sql);
    System.out.println(String.format("Query %d Executed.", queryNum));
    return new ImmutablePair<>(vrs, rs);
  }

  @Test(expected = SQLException.class)
  public void dropTableWithExecuteQueryTest() throws SQLException {
    verdictConn.createStatement().executeQuery("DROP TABLE abc");
  }

  @Test(expected = SQLException.class)
  public void dropTableWithExecuteTest() throws SQLException {
    verdictConn.createStatement().execute("DROP TABLE abc");
  }

  @Test
  public void test1() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(1);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 10) {
        rs.next();
        assertEquals(rs.getString(1), vrs.getString(2));
        assertEquals(rs.getString(2), vrs.getString(3));
        assertEquals(rs.getLong(3), vrs.getLong(4));
        assertEquals(rs.getDouble(4), vrs.getDouble(5), 1e-5);
        assertEquals(rs.getDouble(5), vrs.getDouble(6), 1e-5);
        assertEquals(rs.getDouble(6), vrs.getDouble(7), 1e-5);
        assertEquals(rs.getDouble(7), vrs.getDouble(8), 1e-5);
        assertEquals(rs.getDouble(8), vrs.getDouble(9), 1e-5);
        assertEquals(rs.getDouble(9), vrs.getDouble(10), 1e-5);
        assertEquals(rs.getDouble(10), vrs.getDouble(11), 1e-5);
      }
    }
    // assertEquals(10, blockSeq);
  }

  @Test
  public void test3() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(3);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 12) {
        rs.next();
        assertEquals(rs.getDouble(1), vrs.getDouble(2), 1e-5);
        assertEquals(rs.getDouble(2), vrs.getDouble(3), 1e-5);
        assertEquals(rs.getString(3), vrs.getString(4));
        assertEquals(rs.getString(4), vrs.getString(5));
      }
    }
    assertEquals(12, blockSeq);
  }

  @Test
  public void test4() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(4);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 12) {
        rs.next();
        assertEquals(rs.getString(1), vrs.getString(2));
        assertEquals(rs.getDouble(2), vrs.getDouble(3), 1e-5);
      }
    }
    assertEquals(12, blockSeq);
  }

  @Test
  public void test5() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(5);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 12) {
        rs.next();
        assertEquals(rs.getString(1), vrs.getString(2));
        assertEquals(rs.getDouble(2), vrs.getDouble(3), 1e-5);
      }
    }
    assertEquals(12, blockSeq);
  }

  @Test
  public void test6() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(6);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 10) {
        rs.next();
        assertEquals(rs.getDouble(1), vrs.getDouble(2), 1e-5);
      }
    }
    assertEquals(10, blockSeq);
  }

  @Test
  public void test7() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(7);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 12) {
        rs.next();
        assertEquals(rs.getString(1), vrs.getString(2));
        assertEquals(rs.getString(2), vrs.getString(3));
        assertEquals(rs.getString(3), vrs.getString(4));
        assertEquals(rs.getDouble(4), vrs.getDouble(5), 1e-5);
      }
    }
    assertEquals(12, blockSeq);
  }

  @Test
  public void test8() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(8);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 12) {
        rs.next();
        assertEquals(rs.getString(1), vrs.getString(2));
        assertEquals(rs.getDouble(2), vrs.getDouble(3), 1e-5);
        assertEquals(rs.getDouble(3), vrs.getDouble(4), 1e-5);
      }
    }
    assertEquals(12, blockSeq);
  }

  @Test
  public void test9() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(9);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 12) {
        rs.next();
        assertEquals(rs.getString(1), vrs.getString(2));
        assertEquals(rs.getString(2), vrs.getString(3));
        assertEquals(rs.getDouble(3), vrs.getDouble(4), 1e-5);
      }
    }
    assertEquals(12, blockSeq);
  }

  @Test
  public void test10() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(10);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 12) {
        rs.next();
        assertEquals(rs.getString(1), vrs.getString(2));
        assertEquals(rs.getString(2), vrs.getString(3));
        assertEquals(rs.getDouble(3), vrs.getDouble(4), 1e-5);
      }
    }
    assertEquals(12, blockSeq);
  }

  @Test
  public void test12() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(12);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 12) {
        rs.next();
        assertEquals(rs.getString(1), vrs.getString(2));
        assertEquals(rs.getDouble(2), vrs.getDouble(3), 1e-5);
        assertEquals(rs.getDouble(3), vrs.getDouble(4), 1e-5);
      }
    }
    assertEquals(12, blockSeq);
  }

  @Test
  public void test13() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(13);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 3) {
        rs.next();
        assertEquals(rs.getString(1), vrs.getString(2));
        assertEquals(rs.getDouble(2), vrs.getDouble(3), 1e-5);
      }
    }
    assertEquals(3, blockSeq);
  }

  @Test
  public void test14() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(14);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 10) {
        rs.next();
        assertEquals(rs.getDouble(2), vrs.getDouble(3), 1e-5);
        assertEquals(rs.getDouble(1), vrs.getDouble(2), 1e-5);
      }
    }
    assertEquals(10, blockSeq);
  }

  @Test
  public void test15() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(15);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 10) {
        rs.next();
        assertEquals(rs.getDouble(2), vrs.getDouble(3), 1e-5);
        assertEquals(rs.getDouble(1), vrs.getDouble(2), 1e-5);
      }
    }
    assertEquals(10, blockSeq);
  }

  @Test
  public void test17() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(17);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 10) {
        rs.next();
        assertEquals(rs.getDouble(1), vrs.getDouble(2), 1e-5);
      }
    }
    assertEquals(10, blockSeq);
  }

  @Test
  public void test18() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(18);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 12) {
        rs.next();
        assertEquals(rs.getString(1), vrs.getString(2));
        assertEquals(rs.getString(2), vrs.getString(3));
        assertEquals(rs.getString(3), vrs.getString(4));
        assertEquals(rs.getString(4), vrs.getString(5));
        assertEquals(rs.getString(5), vrs.getString(6));
        assertEquals(rs.getDouble(6), vrs.getDouble(7), 1e-5);
      }
    }
    assertEquals(12, blockSeq);
  }

  @Test
  public void test19() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(19);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 10) {
        rs.next();
        assertEquals(rs.getDouble(1), vrs.getDouble(2), 1e-5);
      }
    }
    assertEquals(10, blockSeq);
  }

  @Test
  public void test20() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(20);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 10) {
        rs.next();
        assertEquals(rs.getString(1), vrs.getString(2));
        assertEquals(rs.getDouble(2), vrs.getDouble(3), 1e-5);
      }
    }
    assertEquals(10, blockSeq);
  }

  @Test
  public void test21() throws SQLException, VerdictDBException, IOException {
    Pair<ResultSet, ResultSet> answerPair = getAnswerPair(21);
    ResultSet vrs = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int blockSeq = 0;
    while (vrs.next()) {
      blockSeq = vrs.getInt(1);
      if (blockSeq == 12) {
        rs.next();
        assertEquals(rs.getString(1), vrs.getString(2));
        assertEquals(rs.getDouble(2), vrs.getDouble(3), 1e-5);
      }
    }
    assertEquals(12, blockSeq);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s`", MYSQL_DATABASE));
  }
}
