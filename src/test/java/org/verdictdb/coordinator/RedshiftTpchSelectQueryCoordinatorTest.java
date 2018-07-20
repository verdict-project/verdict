package org.verdictdb.coordinator;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.RedshiftSyntax;
import org.verdictdb.sqlwriter.SelectQueryToSql;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

/**
 *  Test cases are from
 *  https://github.com/umich-dbgroup/verdictdb-core/wiki/TPCH-Query-Reference--(Experiment-Version)
 *
 *  Some test cases are slightly changed because size of test data are small.
 */
public class RedshiftTpchSelectQueryCoordinatorTest {

  static Connection redshiftConn;

  static DbmsConnection dbmsConn;

  // lineitem has 10 blocks, orders has 3 blocks;
  // lineitem join orders has 12 blocks
  final static int blockSize = 100;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  private static Statement stmt;

  private static final String REDSHIFT_HOST;

  private static final String REDSHIFT_DATABASE = "dev";

  private static final String REDSHIFT_SCHEMA = "tpch";

  private static final String REDSHIFT_USER;

  private static final String REDSHIFT_PASSWORD;

  static {
    REDSHIFT_HOST = System.getenv("VERDICTDB_TEST_REDSHIFT_ENDPOINT");
    REDSHIFT_USER = System.getenv("VERDICTDB_TEST_REDSHIFT_USER");
    REDSHIFT_PASSWORD = System.getenv("VERDICTDB_TEST_REDSHIFT_PASSWORD");
//    System.out.println(REDSHIFT_HOST);
//    System.out.println(REDSHIFT_USER);
//    System.out.println(REDSHIFT_PASSWORD);
  }

  Pair<ExecutionResultReader, ResultSet> getAnswerPair(int queryNum)
      throws VerdictDBException, SQLException, IOException {
    String filename = "query"+queryNum+".sql";
    File file = new File("src/test/resources/tpch_test_query/"+filename);
    String sql = Files.toString(file, Charsets.UTF_8);

    dbmsConn.setDefaultSchema(REDSHIFT_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsConn);
    coordinator.setScrambleMetaSet(meta);
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsConn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new RedshiftSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    ResultSet rs = stmt.executeQuery(stdQuery);
    return new ImmutablePair<>(reader, rs);
  }

  @BeforeClass
  public static void setupRedshiftDatabase() throws SQLException, VerdictDBException, IOException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    redshiftConn =
        DatabaseConnectionHelpers.setupRedshift(
            connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD, REDSHIFT_SCHEMA);
    stmt = redshiftConn.createStatement();

    dbmsConn = new CachedDbmsConnection(
        new JdbcConnection(redshiftConn, new RedshiftSyntax()));
    // Create Scramble table
    stmt.execute(String.format("DROP TABLE IF EXISTS \"%s\".\"lineitem_scrambled\"", REDSHIFT_SCHEMA));
    stmt.execute(String.format("DROP TABLE IF EXISTS \"%s\".\"orders_scrambled\"", REDSHIFT_SCHEMA));

    ScramblingCoordinator scrambler =
        new ScramblingCoordinator(dbmsConn, REDSHIFT_SCHEMA, REDSHIFT_SCHEMA, (long) 100);
    ScrambleMeta meta1 =
        scrambler.scramble(REDSHIFT_SCHEMA, "lineitem", REDSHIFT_SCHEMA, "lineitem_scrambled", "uniform");
    ScrambleMeta meta2 =
        scrambler.scramble(REDSHIFT_SCHEMA, "orders", REDSHIFT_SCHEMA, "orders_scrambled", "uniform");
    meta.addScrambleMeta(meta1);
    meta.addScrambleMeta(meta2);

    stmt.execute("create schema if not exists \"verdictdb_temp\"");
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
    System.out.println("test 1 passed");
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
          assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
          assertEquals(rs.getString(4), dbmsQueryResult.getString(3));
        }
      }
    }
    assertEquals(12, cnt);
    System.out.println("test 3 passed");
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
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
        }
      }
    }
    assertEquals(12, cnt);
    System.out.println("test 4 passed");
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
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
        }
      }
    }
    assertEquals(12, cnt);
    System.out.println("test 5 passed");
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
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
        }
      }
    }
    assertEquals(10, cnt);
    System.out.println("test 6 passed");
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
          assertEquals(rs.getDouble(4), dbmsQueryResult.getDouble(3), 1e-2);
        }
      }
    }
    assertEquals(12, cnt);
    System.out.println("test 7 passed");
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
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
          assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-2);
        }
      }
    }
    assertEquals(12, cnt);
    System.out.println("test 8 passed");
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
          assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-2);
        }
      }
    }
    assertEquals(12, cnt);
    System.out.println("test 9 passed");
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
          assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-2);
        }
      }
    }
    assertEquals(12, cnt);
    System.out.println("test 10 passed");
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
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
          assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-2);
        }
      }
    }
    assertEquals(12, cnt);
    System.out.println("test 12 passed");
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
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
        }
      }
    }
    assertEquals(3, cnt);
    System.out.println("test 13 passed");
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
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
        }
      }
    }
    assertEquals(10, cnt);
    System.out.println("test 14 passed");
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
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
        }
      }
    }
    assertEquals(10, cnt);
    System.out.println("test 15 passed");
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
    System.out.println("test 17 passed");
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
          assertEquals(rs.getDouble(6), dbmsQueryResult.getDouble(5), 1e-2);
        }
      }
    }
    assertEquals(12, cnt);
    System.out.println("test 18 passed");
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
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-2);
        }
      }
    }
    assertEquals(10, cnt);
    System.out.println("test 19 passed");
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
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
        }
      }
    }
    assertEquals(10, cnt);
    System.out.println("test 20 passed");
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
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-2);
        }
      }
    }
    assertEquals(12, cnt);
    System.out.println("test 21 passed");
  }




  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute(String.format("drop schema if exists \"%s\" CASCADE", REDSHIFT_SCHEMA));
  }
}
