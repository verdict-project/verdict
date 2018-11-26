package org.verdictdb.coordinator;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.VerdictResultStream;
import org.verdictdb.VerdictSingleResult;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.MysqlSyntax;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class QueryResultAccuracyEstimatorTest {

  //lineitem has 10 blocks, orders has 3 blocks;
  // lineitem join orders has 12 blocks
  static final int blockSize = 100;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static VerdictOption options = new VerdictOption();

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

  private static final String MYSQL_DATABASE =
      "coordinator_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "";
  
  private static DbmsConnection dbmsconn;

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    conn =
        DatabaseConnectionHelpers.setupMySql(
            mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD, MYSQL_DATABASE);
    conn.setCatalog(MYSQL_DATABASE);
    stmt = conn.createStatement();
    stmt.execute(String.format("use `%s`", MYSQL_DATABASE));
    DbmsConnection dbmsConn = JdbcConnection.create(conn);

    // Create Scramble table
    dbmsConn.execute(
        String.format("DROP TABLE IF EXISTS `%s`.`lineitem_scrambled`", MYSQL_DATABASE));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`orders_scrambled`", MYSQL_DATABASE));
    dbmsConn.execute(
        String.format("DROP TABLE IF EXISTS `%s`.`lineitem_hash_scrambled`", MYSQL_DATABASE));

    ScramblingCoordinator scrambler =
        new ScramblingCoordinator(dbmsConn, MYSQL_DATABASE, MYSQL_DATABASE, (long) 100);
    ScrambleMeta meta1 =
        scrambler.scramble(
            MYSQL_DATABASE, "lineitem", MYSQL_DATABASE, "lineitem_scrambled", "uniform");
    ScrambleMeta meta2 =
        scrambler.scramble(MYSQL_DATABASE, "orders", MYSQL_DATABASE, "orders_scrambled", "uniform");
    ScrambleMeta meta3 =
        scrambler.scramble(
            MYSQL_DATABASE, "lineitem", MYSQL_DATABASE, "lineitem_hash_scrambled", "hash", "l_orderkey");
    meta.addScrambleMeta(meta1);
    meta.addScrambleMeta(meta2);
    meta.addScrambleMeta(meta3);
    stmt.execute(String.format("drop schema if exists `%s`", options.getVerdictTempSchemaName()));
    stmt.execute(
        String.format("create schema if not exists `%s`", options.getVerdictTempSchemaName()));
    
    // Set for Verdict Connection
    JdbcConnection jdbcConn = new JdbcConnection(conn, new MysqlSyntax());
    jdbcConn.setOutputDebugMessage(true);
    dbmsconn = new CachedDbmsConnection(jdbcConn);
    dbmsconn.setDefaultSchema(MYSQL_DATABASE);
  }
  
  public void runQuery(String sql) throws VerdictDBException {
    SelectQuery selectQuery = ExecutionContext.standardizeQuery(sql, dbmsconn);

    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn, options);
    coordinator.setScrambleMetaSet(meta);
    ExecutionResultReader reader = coordinator.process(sql);
    VerdictResultStream stream = new VerdictResultStreamFromExecutionResultReader(reader);

    QueryResultAccuracyEstimator accEst = 
        new QueryResultAccuracyEstimatorFromDifference(selectQuery);

    while (stream.hasNext()) {
      VerdictSingleResult rs = stream.next();
      rs.print();
      rs.rewind();
      accEst.add(rs);
      if (accEst.isLastResultAccurate()) {
        System.out.println("Accurate enough");
      } else {
        System.out.println("NOT accurate enough");
      }
    }
  }

  @Test
  public void query1SqlTest() throws VerdictDBException, SQLException, IOException {
    String filename = "query1.sql";
    File file = new File("src/test/resources/tpch_test_query/" + filename);
    String sql = Files.toString(file, Charsets.UTF_8);
    runQuery(sql);
  }

  @Test
  public void queryCountDistinct1SqlTest() throws VerdictDBException, SQLException, IOException {
    String filename = "query100.sql";
    File file = new File("src/test/resources/tpch_test_query/" + filename);
    String sql = Files.toString(file, Charsets.UTF_8);
    runQuery(sql);
  }
  
}
