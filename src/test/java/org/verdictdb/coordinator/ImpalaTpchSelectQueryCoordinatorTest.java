package org.verdictdb.coordinator;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.ImpalaSyntax;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class ImpalaTpchSelectQueryCoordinatorTest {

  private static Connection impalaConn;

  private static Statement stmt;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  private static Statement impalaStmt;

  private static final String IMPALA_HOST;

  // to avoid possible conflicts among concurrent tests
  private static final String IMPALA_DATABASE =
      "scrambling_coordinator_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String IMPALA_UESR = "";

  private static final String IMPALA_PASSWORD = "";

  private int lastQuery;

  static {
    IMPALA_HOST = System.getenv("VERDICTDB_TEST_IMPALA_HOST");
  }

  @BeforeClass
  public static void setupImpalaDatabase() throws SQLException, VerdictDBException, IOException {
    String impalaConnectionString = String.format("jdbc:impala://%s", IMPALA_HOST);
    impalaConn =
        DatabaseConnectionHelpers.setupImpala(
            impalaConnectionString, IMPALA_UESR, IMPALA_PASSWORD, IMPALA_DATABASE);
    //    impalaStmt = impalaConn.createStatement();
    stmt = impalaConn.createStatement();
    stmt.execute(String.format("use `%s`", IMPALA_DATABASE));
    DbmsConnection dbmsConn = JdbcConnection.create(impalaConn);

    // Create Scramble table
    dbmsConn.execute(
        String.format("DROP TABLE IF EXISTS `%s`.`lineitem_scrambled`", IMPALA_DATABASE));
    dbmsConn.execute(
        String.format("DROP TABLE IF EXISTS `%s`.`orders_scrambled`", IMPALA_DATABASE));

    ScramblingCoordinator scrambler =
        new ScramblingCoordinator(dbmsConn, IMPALA_DATABASE, IMPALA_DATABASE, (long) 100);
    ScrambleMeta meta1 =
        scrambler.scramble(
            IMPALA_DATABASE, "lineitem", IMPALA_DATABASE, "lineitem_scrambled", "uniform");
    ScrambleMeta meta2 =
        scrambler.scramble(
            IMPALA_DATABASE, "orders", IMPALA_DATABASE, "orders_scrambled", "uniform");
    meta.addScrambleMeta(meta1);
    meta.addScrambleMeta(meta2);
    //    stmt.execute("drop schema if exists `verdictdb_temp` CASCADE");
    //    stmt.execute("create schema if not exists `verdictdb_temp`");
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute(String.format("use `%s`", "DEFAULT"));
    impalaConn
        .createStatement()
        .execute(String.format("DROP SCHEMA IF EXISTS `%s` CASCADE", IMPALA_DATABASE));
    impalaConn.close();
  }

  @After
  public void printOutput() {
    System.out.println(String.format("Test %d has passed.", lastQuery));
  }

  Pair<ExecutionResultReader, ResultSet> getAnswerPair(int queryNum)
      throws VerdictDBException, SQLException, IOException {

    lastQuery = queryNum;
    String filename = "query" + queryNum + "_impala" + ".sql";
    File file = new File("src/test/resources/tpch_test_query/" + filename);
    String sql = Files.toString(file, Charsets.UTF_8);
    DbmsConnection dbmsconn =
        new CachedDbmsConnection(new JdbcConnection(impalaConn, new ImpalaSyntax()));
    dbmsconn.setDefaultSchema(IMPALA_DATABASE);

    ResultSet rs = stmt.executeQuery(sql);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);
    coordinator.setScrambleMetaSet(meta);

    if (queryNum >= 100) {
      sql = sql.replaceAll("lineitem", "lineitem_hash_scrambled");
    } else {
      sql = sql.replaceAll("lineitem", "lineitem_scrambled");
    }
    sql = sql.replaceAll("orders", "orders_scrambled");

    ExecutionResultReader reader = coordinator.process(sql);

    return new ImmutablePair<>(reader, rs);
  }

  @Test
  public void query1Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(1);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          assertEquals(rs.getLong(3), dbmsQueryResult.getLong(2));
          assertEquals(rs.getDouble(4), dbmsQueryResult.getDouble(3), 1e-2);
          assertEquals(rs.getDouble(5), dbmsQueryResult.getDouble(4), 1e-2);
          assertEquals(rs.getDouble(6), dbmsQueryResult.getDouble(5), 1e-2);
          assertEquals(rs.getDouble(7), dbmsQueryResult.getDouble(6), 1e-2);
          assertEquals(rs.getDouble(8), dbmsQueryResult.getDouble(7), 1e-2);
          assertEquals(rs.getDouble(9), dbmsQueryResult.getDouble(8), 1e-2);
          assertEquals(rs.getDouble(10), dbmsQueryResult.getDouble(9), 1e-2);
        }
      }
    }
    assertEquals(10, cnt);
  }

  @Test
  public void query3Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(3);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
          assertEquals(rs.getTimestamp(3), dbmsQueryResult.getTimestamp(2));
          assertEquals(rs.getString(4), dbmsQueryResult.getString(3));
        }
      }
    }
    assertEquals(12, cnt);
  }

  @Test
  public void query4Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(4);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }

  @Test
  public void query5Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(5);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }

  @Test
  public void query6Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(6);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
  }

  @Test
  public void query7Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(7);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
          assertEquals(rs.getDouble(4), dbmsQueryResult.getDouble(3), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }

  @Test
  public void query8Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(8);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
          assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }

  @Test
  public void query9Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(9);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }

  @Test
  public void query10Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(10);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }

  @Test
  public void query12Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(12);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
          assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }

  @Test
  public void query13Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(13);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 3) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
        }
      }
    }
    assertEquals(3, cnt);
  }

  @Test
  public void query14Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(14);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
  }

  @Test
  public void query15Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(15);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
  }

  @Test
  public void query17Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(17);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
        }
      }
    }
    assertEquals(10, cnt);
  }

  @Test
  public void query18Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(18);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
          assertEquals(rs.getTimestamp(4), dbmsQueryResult.getTimestamp(3));
          assertEquals(rs.getString(5), dbmsQueryResult.getString(4));
          assertEquals(rs.getDouble(6), dbmsQueryResult.getDouble(5), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }

  @Test
  public void query19Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(19);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
  }

  @Test
  public void query20Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(20);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
  }

  @Test
  public void query21Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(21);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
  }
}
