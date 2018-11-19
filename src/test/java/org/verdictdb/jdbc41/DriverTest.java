package org.verdictdb.jdbc41;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.exception.VerdictDBDbmsException;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class DriverTest {

  private static final String REDSHIFT_HOST;

  private static final String REDSHIFT_DATABASE = "dev";

  private static final String REDSHIFT_SCHEMA =
      "tpch_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String REDSHIFT_USER;

  private static final String REDSHIFT_PASSWORD;

  private static Connection conn;

  private static VerdictOption options = new VerdictOption();

  static {
    REDSHIFT_HOST = System.getenv("VERDICTDB_TEST_REDSHIFT_ENDPOINT");
    REDSHIFT_USER = System.getenv("VERDICTDB_TEST_REDSHIFT_USER");
    REDSHIFT_PASSWORD = System.getenv("VERDICTDB_TEST_REDSHIFT_PASSWORD");
  }

  @BeforeClass
  public static void setup() throws VerdictDBDbmsException, SQLException, IOException {
    options.setVerdictTempSchemaName(REDSHIFT_SCHEMA);
    options.setVerdictMetaSchemaName(REDSHIFT_SCHEMA);
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    conn =
        DatabaseConnectionHelpers.setupRedshift(
            connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD, REDSHIFT_SCHEMA);
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    conn.createStatement()
        .execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", REDSHIFT_SCHEMA));
  }

  @Test
  public void printData() throws SQLException {
    String verdictConnectionString =
        String.format("jdbc:verdict:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    Properties info = new Properties();
    info.setProperty("user", REDSHIFT_USER);
    info.setProperty("password", REDSHIFT_PASSWORD);
    info.setProperty("verdictdbtempschema", REDSHIFT_SCHEMA);
    info.setProperty("verdictdbmetaschema", REDSHIFT_SCHEMA);
    Connection conn = DriverManager.getConnection(verdictConnectionString, info);

    ResultSet rs =
        conn.createStatement()
            .executeQuery(String.format("select * from %s.nation", REDSHIFT_SCHEMA));
    ResultSetMetaData meta = rs.getMetaData();
    //    for (int i = 1; i < meta.getColumnCount()+1; i++) {
    //      System.out.print(meta.getColumnType(i) + " ");
    //    }
    //    System.out.println();
    //
    //    while (rs.next()) {
    //      System.out.print(rs.getInt(1) + "\t");
    //      System.out.print(rs.getString(2) + "\t");
    //      System.out.print(rs.getInt(3) + "\t");
    //      System.out.print(rs.getString(4) + "\t");
    //      System.out.print(rs.getString(5) + "\t");
    //      System.out.println();
    //    }
    //    DbmsQueryResult qr = new JdbcQueryResult(rs);
    //    qr.printContent();
  }

  @Test
  public void testChangingDefaultSchema() throws SQLException {
    String verdictConnectionString =
        String.format("jdbc:verdict:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    Properties info = new Properties();
    info.setProperty("user", REDSHIFT_USER);
    info.setProperty("password", REDSHIFT_PASSWORD);
    info.setProperty("verdictdbmetaschema", REDSHIFT_SCHEMA);
    Connection conn = DriverManager.getConnection(verdictConnectionString, info);

    String schema = conn.getSchema();
    assertEquals("public", schema);

    conn.createStatement().execute("use myschema");
    String newSchema = conn.getSchema();
    assertEquals("myschema", newSchema);
  }

  @Test
  public void testConnectionWithProperties() throws SQLException {
    String verdictConnectionString =
        String.format("jdbc:verdict:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    Properties info = new Properties();
    info.setProperty("user", REDSHIFT_USER);
    info.setProperty("password", REDSHIFT_PASSWORD);
    info.setProperty("verdictdbmetaschema", REDSHIFT_SCHEMA);
    Connection conn = DriverManager.getConnection(verdictConnectionString, info);
  }

  //  @Test
  public void redshift15() throws IOException, SQLException, VerdictDBDbmsException {
    String filename = "companya/redshift_queries/15.sql";
    File queryFile = new File(getClass().getClassLoader().getResource(filename).getFile());
    String sql = Files.toString(queryFile, Charsets.UTF_8);

    String onnectionString =
        String.format("jdbc:verdict:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    Properties info = new Properties();
    info.setProperty("user", REDSHIFT_USER);
    info.setProperty("password", REDSHIFT_PASSWORD);
    Connection conn = DriverManager.getConnection(onnectionString, info);
    conn.createStatement().execute(sql);

    //    JdbcConnection dbmsConn = JdbcConnection.create(conn);
    //    dbmsConn.setOutputDebugMessage(true);
    //    dbmsConn.execute(sql);

  }
}
