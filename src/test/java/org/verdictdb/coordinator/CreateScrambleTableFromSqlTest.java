package org.verdictdb.coordinator;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.metastore.ScrambleMetaStore;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Created by Dong Young Yoon on 7/26/18. */
@RunWith(Parameterized.class)
public class CreateScrambleTableFromSqlTest {

  private static final String[] targetDatabases = {"mysql", "impala", "redshift", "postgresql"};
  //  private static final String[] targetDatabases = {"mysql"};

  private static Map<String, Pair<Connection, Connection>> connections = new HashMap<>();

  private String database;

  private static VerdictOption options = new VerdictOption();

  private static final String TEMP_SCHEMA_NAME =
      "verdictdb_temp_schema_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  public CreateScrambleTableFromSqlTest(String database) {
    this.database = database;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> databases() {
    Collection<Object[]> params = new ArrayList<>();
    for (String database : targetDatabases) {
      params.add(new Object[] {database});
    }
    return params;
  }

  @BeforeClass
  public static void setup() throws VerdictDBDbmsException, SQLException, IOException {
    options.setVerdictTempSchemaName(TEMP_SCHEMA_NAME);
    options.setVerdictMetaSchemaName(TEMP_SCHEMA_NAME);
    setupMysql();
    setupImpala();
    setupRedshift();
    setupPostgresql();
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    for (String database : connections.keySet()) {
      Pair<Connection, Connection> pair = connections.get(database);
      if (pair != null) {
        if (database.equals("mysql")) {
          pair.getLeft()
              .createStatement()
              .execute(
                  String.format(
                      "DROP SCHEMA IF EXISTS %s", DatabaseConnectionHelpers.COMMON_SCHEMA_NAME));
          pair.getLeft()
              .createStatement()
              .execute(String.format("DROP SCHEMA IF EXISTS %s", TEMP_SCHEMA_NAME));
        } else {
          pair.getLeft()
              .createStatement()
              .execute(
                  String.format(
                      "DROP SCHEMA IF EXISTS %s CASCADE",
                      DatabaseConnectionHelpers.COMMON_SCHEMA_NAME));
          pair.getLeft()
              .createStatement()
              .execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", TEMP_SCHEMA_NAME));
        }
        pair.getLeft().close();
        pair.getRight().close();
      }
    }
  }

  private static void setupMysql() throws VerdictDBDbmsException, SQLException {
    String mysqlConnectionString =
        String.format(
            "jdbc:mysql://%s?autoReconnect=true&useSSL=false",
            DatabaseConnectionHelpers.MYSQL_HOST);
    String vcMysqlConnectionString =
        String.format(
            "jdbc:verdict:mysql://%s?autoReconnect=true&useSSL=false&verdictdbtempschema=%s&verdictdbmetaschema=%s",
            DatabaseConnectionHelpers.MYSQL_HOST, TEMP_SCHEMA_NAME, TEMP_SCHEMA_NAME);
    Connection conn =
        DatabaseConnectionHelpers.setupMySql(
            mysqlConnectionString,
            DatabaseConnectionHelpers.MYSQL_USER,
            DatabaseConnectionHelpers.MYSQL_PASSWORD,
            DatabaseConnectionHelpers.COMMON_SCHEMA_NAME);
    Connection vc =
        DriverManager.getConnection(
            vcMysqlConnectionString,
            DatabaseConnectionHelpers.MYSQL_USER,
            DatabaseConnectionHelpers.MYSQL_PASSWORD);

    conn.createStatement().execute(String.format("DROP SCHEMA IF EXISTS %s", TEMP_SCHEMA_NAME));
    conn.createStatement()
        .execute(String.format("CREATE SCHEMA IF NOT EXISTS %s", TEMP_SCHEMA_NAME));

    connections.put("mysql", ImmutablePair.of(conn, vc));
  }

  private static void setupImpala() throws VerdictDBDbmsException, SQLException, IOException {
    String connectionString =
        String.format("jdbc:impala://%s", DatabaseConnectionHelpers.IMPALA_HOST);
    String vcConnectionString =
        String.format(
            "jdbc:verdict:impala://%s;verdictdbtempschema=%s;verdictdbmetaschema=%s",
            DatabaseConnectionHelpers.IMPALA_HOST, TEMP_SCHEMA_NAME, TEMP_SCHEMA_NAME);
    Connection conn =
        DatabaseConnectionHelpers.setupImpala(
            connectionString,
            DatabaseConnectionHelpers.IMPALA_USER,
            DatabaseConnectionHelpers.IMPALA_PASSWORD,
            DatabaseConnectionHelpers.COMMON_SCHEMA_NAME);
    Connection vc =
        DriverManager.getConnection(
            vcConnectionString,
            DatabaseConnectionHelpers.IMPALA_USER,
            DatabaseConnectionHelpers.IMPALA_PASSWORD);

    conn.createStatement()
        .execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", TEMP_SCHEMA_NAME));
    conn.createStatement()
        .execute(String.format("CREATE SCHEMA IF NOT EXISTS %s", TEMP_SCHEMA_NAME));

    connections.put("impala", ImmutablePair.of(conn, vc));
  }

  private static void setupRedshift() throws VerdictDBDbmsException, SQLException, IOException {

    String connectionString =
        String.format(
            "jdbc:redshift://%s/%s",
            DatabaseConnectionHelpers.REDSHIFT_HOST, DatabaseConnectionHelpers.REDSHIFT_DATABASE);
    String vcConnectionString =
        String.format(
            "jdbc:verdict:redshift://%s/%s;verdictdbtempschema=%s;verdictdbmetaschema=%s",
            DatabaseConnectionHelpers.REDSHIFT_HOST,
            DatabaseConnectionHelpers.REDSHIFT_DATABASE,
            TEMP_SCHEMA_NAME,
            TEMP_SCHEMA_NAME);
    Connection conn =
        DatabaseConnectionHelpers.setupRedshift(
            connectionString,
            DatabaseConnectionHelpers.REDSHIFT_USER,
            DatabaseConnectionHelpers.REDSHIFT_PASSWORD,
            DatabaseConnectionHelpers.COMMON_SCHEMA_NAME);
    Connection vc =
        DriverManager.getConnection(
            vcConnectionString,
            DatabaseConnectionHelpers.REDSHIFT_USER,
            DatabaseConnectionHelpers.REDSHIFT_PASSWORD);

    conn.createStatement()
        .execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", TEMP_SCHEMA_NAME));
    conn.createStatement()
        .execute(String.format("CREATE SCHEMA IF NOT EXISTS %s", TEMP_SCHEMA_NAME));

    connections.put("redshift", ImmutablePair.of(conn, vc));
  }

  private static void setupPostgresql() throws SQLException, VerdictDBDbmsException, IOException {

    String connectionString =
        String.format(
            "jdbc:postgresql://%s/%s",
            DatabaseConnectionHelpers.POSTGRES_HOST, DatabaseConnectionHelpers.POSTGRES_DATABASE);
    String vcConnectionString =
        String.format(
            "jdbc:verdict:postgresql://%s/%s?verdictdbtempschema=%s",
            DatabaseConnectionHelpers.POSTGRES_HOST,
            DatabaseConnectionHelpers.POSTGRES_DATABASE,
            TEMP_SCHEMA_NAME);
    Connection conn =
        DatabaseConnectionHelpers.setupPostgresql(
            connectionString,
            DatabaseConnectionHelpers.POSTGRES_USER,
            DatabaseConnectionHelpers.POSTGRES_PASSWORD,
            DatabaseConnectionHelpers.COMMON_SCHEMA_NAME);
    Properties prop = new Properties();
    prop.setProperty("user", DatabaseConnectionHelpers.POSTGRES_USER);
    prop.setProperty("password", DatabaseConnectionHelpers.POSTGRES_PASSWORD);
    prop.setProperty("verdictdbmetaschema", TEMP_SCHEMA_NAME);
    prop.setProperty("verdictdbtempschema", TEMP_SCHEMA_NAME);
    Connection vc = DriverManager.getConnection(vcConnectionString, prop);

    conn.createStatement()
        .execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", TEMP_SCHEMA_NAME));
    conn.createStatement()
        .execute(String.format("CREATE SCHEMA IF NOT EXISTS %s", TEMP_SCHEMA_NAME));

    connections.put("postgresql", ImmutablePair.of(conn, vc));
  }

  @Test
  public void createUniformScrambleTableTest() throws SQLException, VerdictDBException {
    Connection conn = connections.get(database).getLeft();
    Connection vc = connections.get(database).getRight();
    JdbcConnection jdbcConn = JdbcConnection.create(conn);
    ScrambleMetaStore store = new ScrambleMetaStore(jdbcConn, options);
    store.remove();
    String createScrambleSql =
        String.format(
            "CREATE SCRAMBLE %s.lineitem_uniform_scramble FROM %s.lineitem METHOD 'uniform'",
            TEMP_SCHEMA_NAME, DatabaseConnectionHelpers.COMMON_SCHEMA_NAME);

    Statement stmt = vc.createStatement();
    stmt.execute(createScrambleSql);

    String countOriginalSql =
        String.format(
            "SELECT COUNT(*) FROM %s.lineitem", DatabaseConnectionHelpers.COMMON_SCHEMA_NAME);
    String countScrambleSql =
        String.format("SELECT COUNT(*) FROM %s.lineitem_uniform_scramble", TEMP_SCHEMA_NAME);

    ResultSet rs1 = conn.createStatement().executeQuery(countOriginalSql);
    ResultSet rs2 = conn.createStatement().executeQuery(countScrambleSql);

    assertEquals(rs1.next(), rs2.next());
    assertEquals(rs1.getLong(1), rs2.getLong(1));

    ScrambleMetaSet scrambleMetadata = store.retrieve();
    ScrambleMeta meta =
        scrambleMetadata.getMetaForTable(TEMP_SCHEMA_NAME, "lineitem_uniform_scramble");
    assertEquals(DatabaseConnectionHelpers.COMMON_SCHEMA_NAME, meta.getOriginalSchemaName());
    assertEquals("lineitem", meta.getOriginalTableName());
    assertEquals(TEMP_SCHEMA_NAME, meta.getSchemaName());
    assertEquals("lineitem_uniform_scramble", meta.getTableName());
    assertEquals("uniform", meta.getMethod());
  }

  @Test
  public void createFastConvergeScrambleTableTest() throws SQLException, VerdictDBException {
    Connection conn = connections.get(database).getLeft();
    Connection vc = connections.get(database).getRight();
    JdbcConnection jdbcConn = JdbcConnection.create(conn);
    ScrambleMetaStore store = new ScrambleMetaStore(jdbcConn, options);
    store.remove();
    String createScrambleSql =
        String.format(
            "CREATE SCRAMBLE %s.lineitem_fc_scramble FROM %s.lineitem METHOD 'fastconverge'",
            TEMP_SCHEMA_NAME, DatabaseConnectionHelpers.COMMON_SCHEMA_NAME);

    Statement stmt = vc.createStatement();
    stmt.execute(createScrambleSql);

    String countOriginalSql =
        String.format(
            "SELECT COUNT(*) FROM %s.lineitem", DatabaseConnectionHelpers.COMMON_SCHEMA_NAME);
    String countScrambleSql =
        String.format("SELECT COUNT(*) FROM %s.lineitem_fc_scramble", TEMP_SCHEMA_NAME);

    ResultSet rs1 = conn.createStatement().executeQuery(countOriginalSql);
    ResultSet rs2 = conn.createStatement().executeQuery(countScrambleSql);

    assertEquals(rs1.next(), rs2.next());
    assertEquals(rs1.getLong(1), rs2.getLong(1));

    ScrambleMetaSet scrambleMetadata = store.retrieve();
    ScrambleMeta meta = scrambleMetadata.getMetaForTable(TEMP_SCHEMA_NAME, "lineitem_fc_scramble");
    assertEquals(DatabaseConnectionHelpers.COMMON_SCHEMA_NAME, meta.getOriginalSchemaName());
    assertEquals("lineitem", meta.getOriginalTableName());
    assertEquals(TEMP_SCHEMA_NAME, meta.getSchemaName());
    assertEquals("lineitem_fc_scramble", meta.getTableName());
    assertEquals("fastconverge", meta.getMethod());
  }

  @Test
  public void replaceUniformScrambleTableTest() throws SQLException, VerdictDBException {
    Connection conn = connections.get(database).getLeft();
    Connection vc = connections.get(database).getRight();
    JdbcConnection jdbcConn = JdbcConnection.create(conn);
    ScrambleMetaStore store = new ScrambleMetaStore(jdbcConn, options);
    store.remove();
    // drop existing table
    String dropScrambleSql =
        String.format("DROP TABLE IF EXISTS %s.lineitem_uniform_scramble", TEMP_SCHEMA_NAME);
    conn.createStatement().execute(dropScrambleSql);

    String createScrambleSql =
        String.format(
            "CREATE SCRAMBLE %s.lineitem_uniform_scramble FROM %s.lineitem " + "METHOD 'uniform'",
            TEMP_SCHEMA_NAME, DatabaseConnectionHelpers.COMMON_SCHEMA_NAME);

    Statement stmt = vc.createStatement();
    stmt.execute(createScrambleSql);

    String countOriginalSql =
        String.format(
            "SELECT COUNT(*) FROM %s.lineitem_uniform_scramble LIMIT 1", TEMP_SCHEMA_NAME);

    //            DatabaseConnectionHelpers.COMMON_SCHEMA_NAME);

    ScrambleMetaSet scrambleMetaSet = store.retrieve();
    SelectQueryCoordinator coordinator =
        new SelectQueryCoordinator(jdbcConn, scrambleMetaSet, options);
    ExecutionResultReader reader = coordinator.process(countOriginalSql);
    while (reader.hasNext()) {
      reader.next();
    }
    SelectQuery query = coordinator.getLastQuery();

    AbstractRelation table = query.getFromList().get(0);

    assertTrue(table instanceof BaseTable);
    BaseTable bt = (BaseTable) table;
    assertEquals(TEMP_SCHEMA_NAME, bt.getSchemaName());
    assertEquals("lineitem_uniform_scramble", bt.getTableName());
  }

  @Test
  public void replaceFastConvergeScrambleTableTest() throws SQLException, VerdictDBException {
    Connection conn = connections.get(database).getLeft();
    Connection vc = connections.get(database).getRight();
    JdbcConnection jdbcConn = JdbcConnection.create(conn);
    ScrambleMetaStore store = new ScrambleMetaStore(jdbcConn, options);
    store.remove();
    // drop existing table
    String dropScrambleSql =
        String.format("DROP TABLE IF EXISTS %s.lineitem_fc_scramble", TEMP_SCHEMA_NAME);
    conn.createStatement().execute(dropScrambleSql);

    String createScrambleSql =
        String.format(
            "CREATE SCRAMBLE %s.lineitem_fc_scramble FROM %s.lineitem " + "METHOD 'fastconverge'",
            TEMP_SCHEMA_NAME, DatabaseConnectionHelpers.COMMON_SCHEMA_NAME);

    Statement stmt = vc.createStatement();
    stmt.execute(createScrambleSql);

    String countOriginalSql =
        String.format("SELECT COUNT(*) FROM %s.lineitem_fc_scramble LIMIT 1", TEMP_SCHEMA_NAME);
    //            DatabaseConnectionHelpers.COMMON_SCHEMA_NAME);

    ScrambleMetaSet scrambleMetaSet = store.retrieve();
    SelectQueryCoordinator coordinator =
        new SelectQueryCoordinator(jdbcConn, scrambleMetaSet, options);
    ExecutionResultReader reader = coordinator.process(countOriginalSql);
    while (reader.hasNext()) {
      reader.next();
    }
    SelectQuery query = coordinator.getLastQuery();

    AbstractRelation table = query.getFromList().get(0);

    assertTrue(table instanceof BaseTable);
    BaseTable bt = (BaseTable) table;
    assertEquals(TEMP_SCHEMA_NAME, bt.getSchemaName());
    assertEquals("lineitem_fc_scramble", bt.getTableName());
  }
}
