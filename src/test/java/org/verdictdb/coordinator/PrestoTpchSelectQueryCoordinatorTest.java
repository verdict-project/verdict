package org.verdictdb.coordinator;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.verdictdb.category.PrestoTests;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.PrestoHiveSyntax;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(PrestoTests.class)
public class PrestoTpchSelectQueryCoordinatorTest {

  // lineitem has 10 blocks, orders has 3 blocks;
  // lineitem join orders has 12 blocks
  static final int blockSize = 100;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static VerdictOption options = new VerdictOption();

  static Connection conn;

  private static Statement stmt;

  private static final String PRESTO_HOST;

  private static final String PRESTO_CATALOG;

  private static final String PRESTO_USER;

  private static final String PRESTO_PASSWORD;

  static {
    PRESTO_HOST = System.getenv("VERDICTDB_TEST_PRESTO_HOST");
    PRESTO_CATALOG = System.getenv("VERDICTDB_TEST_PRESTO_CATALOG");
    PRESTO_USER = System.getenv("VERDICTDB_TEST_PRESTO_USER");
    PRESTO_PASSWORD = System.getenv("VERDICTDB_TEST_PRESTO_PASSWORD");
  }

  private static final String PRESTO_SCHEMA =
      "coordinator_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  @BeforeClass
  public static void setupPrestoDatabase() throws SQLException, VerdictDBException, IOException {
    String prestoConnectionString =
        String.format("jdbc:presto://%s/%s/default", PRESTO_HOST, PRESTO_CATALOG);
    conn =
        DatabaseConnectionHelpers.setupPresto(
            prestoConnectionString, PRESTO_USER, PRESTO_PASSWORD, PRESTO_SCHEMA);
    conn.setCatalog(PRESTO_CATALOG);
    stmt = conn.createStatement();
    stmt.execute(String.format("use %s", PRESTO_SCHEMA));
    DbmsConnection dbmsConn = JdbcConnection.create(conn);

    // Create Scramble table
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS %s.lineitem_scrambled", PRESTO_SCHEMA));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS %s.orders_scrambled", PRESTO_SCHEMA));
    dbmsConn.execute(
        String.format("DROP TABLE IF EXISTS %s.lineitem_hash_scrambled", PRESTO_SCHEMA));

    ScramblingCoordinator scrambler =
        new ScramblingCoordinator(dbmsConn, PRESTO_SCHEMA, PRESTO_SCHEMA, (long) 100);
    ScrambleMeta meta1 =
        scrambler.scramble(
            PRESTO_SCHEMA, "lineitem", PRESTO_SCHEMA, "lineitem_scrambled", "uniform");
    ScrambleMeta meta2 =
        scrambler.scramble(PRESTO_SCHEMA, "orders", PRESTO_SCHEMA, "orders_scrambled", "uniform");
    ScrambleMeta meta3 =
        scrambler.scramble(
            PRESTO_SCHEMA,
            "lineitem",
            PRESTO_SCHEMA,
            "lineitem_hash_scrambled",
            "hash",
            "l_orderkey");
    meta.addScrambleMeta(meta1);
    meta.addScrambleMeta(meta2);
    meta.addScrambleMeta(meta3);
  }

  Pair<ExecutionResultReader, ResultSet> getAnswerPair(int queryNum)
      throws VerdictDBException, SQLException, IOException {
    String filename = "query" + queryNum + ".sql";
    File file = new File("src/test/resources/tpch_test_query/presto/" + filename);
    String sql = Files.toString(file, Charsets.UTF_8);
    JdbcConnection jdbcConn = new JdbcConnection(conn, new PrestoHiveSyntax());
    jdbcConn.setOutputDebugMessage(true);
    DbmsConnection dbmsconn = new CachedDbmsConnection(jdbcConn);
    dbmsconn.setDefaultSchema(PRESTO_SCHEMA);
    ResultSet rs = stmt.executeQuery(sql);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn, options);
    coordinator.setScrambleMetaSet(meta);

    if (queryNum >= 100) {
      sql = sql.replaceAll("lineitem", "lineitem_hash_scrambled");
    } else {
      sql = sql.replaceAll("lineitem", "lineitem_scrambled");
    }
    sql = sql.replaceAll("orders", "orders_scrambled");

    ExecutionResultReader reader = coordinator.process(sql);

    System.out.println(String.format("Query %d Executed.", queryNum));
    return new ImmutablePair<>(reader, rs);
  }

  private static double convertObjectToDouble(Object o) {
    if (o instanceof String) {
      return Double.valueOf((String) o);
    } else if (o instanceof Number) {
      return ((Number) o).doubleValue();
    }
    return 0;
  }

  private static long convertObjectToLong(Object o) {
    if (o instanceof String) {
      return Long.valueOf((String) o);
    } else if (o instanceof Number) {
      return ((Number) o).longValue();
    }
    return 0;
  }

  @Test
  public void queryCountDistinct1Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(100);
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
          assertEquals(convertObjectToLong(rs.getObject(3)), dbmsQueryResult.getLong(2));
        }
      }
    }
    assertEquals(10, cnt);
  }

  @Test
  public void queryCountDistinct2Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(101);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;

      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(convertObjectToLong(rs.getLong(1)), dbmsQueryResult.getLong(0));
        }
      }
    }
    assertEquals(10, cnt);
  }

  @Test
  public void queryCountDistinct3Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(102);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;

      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(convertObjectToLong(rs.getLong(1)), dbmsQueryResult.getLong(0));
        }
      }
    }
    assertEquals(10, cnt);
  }

  @Test
  public void queryCountDistinct4Test() throws VerdictDBException, SQLException, IOException {
    Pair<ExecutionResultReader, ResultSet> answerPair = getAnswerPair(103);
    ExecutionResultReader reader = answerPair.getLeft();
    ResultSet rs = answerPair.getRight();
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;

      // Note: due to the property of approx_distinct
      // the sum is not exactly equal to the original approx_distinct
      if (cnt == 10) {
        while (rs.next()) {
          dbmsQueryResult.next();
          long lowerBound = (long) (dbmsQueryResult.getLong(0) * 0.8);
          long upperBound = (long) (dbmsQueryResult.getLong(0) * 1.2);
          assertTrue(convertObjectToLong(rs.getLong(1)) >= lowerBound);
          assertTrue(convertObjectToLong(rs.getLong(1)) <= upperBound);
        }
      }
    }
    assertEquals(10, cnt);
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
          assertEquals(convertObjectToLong(rs.getObject(3)), dbmsQueryResult.getLong(2));
          assertEquals(convertObjectToDouble(rs.getObject(4)), dbmsQueryResult.getDouble(3), 1);
          assertEquals(convertObjectToDouble(rs.getObject(5)), dbmsQueryResult.getDouble(4), 1);
          assertEquals(convertObjectToDouble(rs.getObject(6)), dbmsQueryResult.getDouble(5), 1);
          assertEquals(convertObjectToDouble(rs.getObject(7)), dbmsQueryResult.getDouble(6), 1);
          assertEquals(convertObjectToDouble(rs.getObject(8)), dbmsQueryResult.getDouble(7), 1);
          assertEquals(convertObjectToDouble(rs.getObject(9)), dbmsQueryResult.getDouble(8), 1);
          assertEquals(convertObjectToDouble(rs.getObject(10)), dbmsQueryResult.getDouble(9), 1);
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
          assertEquals(convertObjectToDouble(rs.getObject(1)), dbmsQueryResult.getDouble(0), 1);
          assertEquals(convertObjectToDouble(rs.getObject(2)), dbmsQueryResult.getDouble(1), 1);
          assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
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
          assertEquals(convertObjectToDouble(rs.getObject(2)), dbmsQueryResult.getDouble(1), 1);
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
          assertEquals(convertObjectToDouble(rs.getObject(2)), dbmsQueryResult.getDouble(1), 1);
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
          assertEquals(convertObjectToDouble(rs.getObject(1)), dbmsQueryResult.getDouble(0), 1);
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
          assertEquals(convertObjectToDouble(rs.getObject(4)), dbmsQueryResult.getDouble(3), 1);
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
          assertEquals(convertObjectToDouble(rs.getObject(2)), dbmsQueryResult.getDouble(1), 1);
          assertEquals(convertObjectToDouble(rs.getObject(3)), dbmsQueryResult.getDouble(2), 1);
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
          assertEquals(convertObjectToDouble(rs.getObject(3)), dbmsQueryResult.getDouble(2), 1);
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
          assertEquals(convertObjectToDouble(rs.getObject(3)), dbmsQueryResult.getDouble(2), 1);
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
          assertEquals(convertObjectToDouble(rs.getObject(2)), dbmsQueryResult.getDouble(1), 1);
          assertEquals(convertObjectToDouble(rs.getObject(3)), dbmsQueryResult.getDouble(2), 1);
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
          assertEquals(convertObjectToDouble(rs.getObject(2)), dbmsQueryResult.getDouble(1), 1);
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
          assertEquals(convertObjectToDouble(rs.getObject(2)), dbmsQueryResult.getDouble(1), 1);
          assertEquals(convertObjectToDouble(rs.getObject(1)), dbmsQueryResult.getDouble(0), 1);
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
          assertEquals(convertObjectToDouble(rs.getObject(2)), dbmsQueryResult.getDouble(1), 1);
          assertEquals(convertObjectToDouble(rs.getObject(1)), dbmsQueryResult.getDouble(0), 1);
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
          assertEquals(convertObjectToDouble(rs.getObject(1)), dbmsQueryResult.getDouble(0), 1);
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
          assertEquals(rs.getString(4), dbmsQueryResult.getString(3));
          assertEquals(rs.getString(5), dbmsQueryResult.getString(4));
          assertEquals(convertObjectToDouble(rs.getObject(6)), dbmsQueryResult.getDouble(5), 1);
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
          assertEquals(convertObjectToDouble(rs.getObject(1)), dbmsQueryResult.getDouble(0), 1);
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
          assertEquals(convertObjectToDouble(rs.getObject(2)), dbmsQueryResult.getDouble(1), 1);
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
          assertEquals(convertObjectToDouble(rs.getObject(2)), dbmsQueryResult.getDouble(1), 1);
        }
      }
    }
    assertEquals(12, cnt);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    ResultSet rs = stmt.executeQuery(String.format("SHOW TABLES IN %s", PRESTO_SCHEMA));
    while (rs.next()) {
      stmt.execute(String.format("DROP TABLE IF EXISTS %s.%s", PRESTO_SCHEMA, rs.getString(1)));
    }
    rs.close();
    stmt.execute(String.format("DROP SCHEMA IF EXISTS %s", PRESTO_SCHEMA));
    stmt.close();
    conn.close();
  }
}
