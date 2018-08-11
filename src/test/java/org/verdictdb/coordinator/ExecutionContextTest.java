package org.verdictdb.coordinator;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.VerdictContext;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.metastore.ScrambleMetaStore;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExecutionContextTest {

  // lineitem has 10 blocks, orders has 3 blocks;
  // lineitem join orders has 12 blocks
  static final int blockSize = 100;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static VerdictOption options = new VerdictOption();

  static Connection conn;

  private static final String MYSQL_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_DATABASE = "exec_context_test";

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "";

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    conn =
        DatabaseConnectionHelpers.setupMySql(
            mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD, MYSQL_DATABASE);
    DbmsConnection dbmsConn = JdbcConnection.create(conn);

    // Create Scramble table
    dbmsConn.execute(
        String.format("DROP TABLE IF EXISTS `%s`.`lineitem_scrambled`", MYSQL_DATABASE));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`orders_scrambled`", MYSQL_DATABASE));

    ScramblingCoordinator scrambler =
        new ScramblingCoordinator(dbmsConn, MYSQL_DATABASE, MYSQL_DATABASE, (long) 100);
    ScrambleMeta meta1 =
        scrambler.scramble(
            MYSQL_DATABASE, "lineitem", MYSQL_DATABASE, "lineitem_scrambled", "uniform");
    ScrambleMeta meta2 =
        scrambler.scramble(MYSQL_DATABASE, "orders", MYSQL_DATABASE, "orders_scrambled", "uniform");
    meta.addScrambleMeta(meta1);
    meta.addScrambleMeta(meta2);
    ScrambleMetaStore store = new ScrambleMetaStore(dbmsConn, options);
    store.addToStore(meta);
    dbmsConn.execute(
        String.format("drop schema if exists `%s`", options.getVerdictTempSchemaName()));
    dbmsConn.execute(
        String.format("create schema if not exists `%s`", options.getVerdictTempSchemaName()));
  }

  @AfterClass
  public static void tearDown() throws VerdictDBDbmsException {
    DbmsConnection dbmsConn = JdbcConnection.create(conn);
    dbmsConn.execute(
        String.format("DROP TABLE IF EXISTS `%s`.`lineitem_scrambled`", MYSQL_DATABASE));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`orders_scrambled`", MYSQL_DATABASE));
  }

  @Test
  public void testSelectQuery() throws VerdictDBException, SQLException {
    String sql = String.format("select avg(l_quantity) from %s.lineitem_scrambled", MYSQL_DATABASE);
    DbmsConnection dbmsConn = JdbcConnection.create(conn);
    VerdictContext verdict = new VerdictContext(dbmsConn);
    ExecutionContext exec = new ExecutionContext(dbmsConn, verdict.getContextId(), 0, options);

    VerdictSingleResult result = exec.sql(sql);
  }

  @Test
  public void testTempTableExistsBeforeTerminate()
      throws VerdictDBException, SQLException, IOException {
    String filename = "query1.sql";
    File file = new File("src/test/resources/tpch_test_query/" + filename);
    String sql = Files.toString(file, Charsets.UTF_8);
    DbmsConnection dbmsConn = JdbcConnection.create(conn);
    ((JdbcConnection) dbmsConn).setOutputDebugMessage(true);
    dbmsConn.setDefaultSchema(MYSQL_DATABASE);
    VerdictContext verdict = new VerdictContext(dbmsConn, options);
    ExecutionContext exec = new ExecutionContext(dbmsConn, verdict.getContextId(), 0, options);

    VerdictResultStream result = exec.streamsql(sql);

    while (result.hasNext()) {
      result.next();
    }

    int tempCount = 0;
    conn.setCatalog(options.getVerdictTempSchemaName());
    ResultSet tables = conn.createStatement().executeQuery("show tables");
    while (tables.next()) {
      String tableName = tables.getString(1);
      if (tableName.startsWith(VerdictOption.getVerdictTempTablePrefix())) {
        ++tempCount;
      }
    }
    assertTrue(0 < tempCount);
    exec.terminate();
  }

  @Test
  public void testTempTableClearAfterTerminate()
      throws VerdictDBException, SQLException, IOException {
    String filename = "query1.sql";
    File file = new File("src/test/resources/tpch_test_query/" + filename);
    String sql = Files.toString(file, Charsets.UTF_8);
    DbmsConnection dbmsConn = JdbcConnection.create(conn);
    dbmsConn.setDefaultSchema(MYSQL_DATABASE);
    VerdictContext verdict = new VerdictContext(dbmsConn, options);
    ExecutionContext exec = new ExecutionContext(dbmsConn, verdict.getContextId(), 0, options);

    VerdictResultStream result = exec.streamsql(sql);

    while (result.hasNext()) {
      result.next();
    }

    exec.terminate();

    int tempCount = 0;
    ResultSet tables =
        conn.createStatement()
            .executeQuery(String.format("show tables in %s", options.getVerdictTempSchemaName()));
    while (tables.next()) {
      String tableName = tables.getString(1);
      if (tableName.startsWith(VerdictOption.getVerdictTempTablePrefix())) {
        System.out.println(tableName);
        ++tempCount;
      }
    }
    assertEquals(0, tempCount);
  }
}
