package org.verdictdb.jdbc41;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.postgresql.jdbc.PgSQLXML;
import org.verdictdb.category.PrestoTests;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.exception.VerdictDBException;

/**
 * Similar to JdbcQueryDataTypeForAllDatabasesTest, but uses a different class to exclude this
 * test from JDK7.
 * 
 * @author Yongjoo Park
 *
 */
@Category(PrestoTests.class)
@RunWith(Parameterized.class)
public class JdbcQueryDataTypeForPrestoTest {
  
  private static Map<String, Connection> connMap = new HashMap<>();

  private static Map<String, VerdictConnection> vcMap = new HashMap<>();

  private static Map<String, String> schemaMap = new HashMap<>();

  private static final String[] targetDatabases = {"presto"};
  
  private String database;
  
  // presto
  private static final String PRESTO_USER;
  
  private static final String PRESTO_HOST;
  
  private static final String PRESTO_CATALOG;
  
  private static final String PRESTO_PASSWORD = "";
  
  private static final String VERDICT_META_SCHEMA =
      "verdictdbmetaschema_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String VERDICT_TEMP_SCHEMA =
      "verdictdbtempschema_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static VerdictOption options = new VerdictOption();
  
  static {
    PRESTO_HOST = System.getenv("VERDICTDB_TEST_PRESTO_HOST");
    PRESTO_CATALOG = System.getenv("VERDICTDB_TEST_PRESTO_CATALOG");
    PRESTO_USER = System.getenv("VERDICTDB_TEST_PRESTO_USER");
  }
  
  private static final String SCHEMA_NAME =
      "data_type_test" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  private static final String TABLE_NAME =
      "data_type_test" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
  
  public JdbcQueryDataTypeForPrestoTest(String database) {
    this.database = database;
  }
  
  @BeforeClass
  public static void setup() throws SQLException, VerdictDBException {
    options.setVerdictMetaSchemaName(VERDICT_META_SCHEMA);
    options.setVerdictTempSchemaName(VERDICT_TEMP_SCHEMA);
    setupPresto();
  }
  
  @AfterClass
  public static void tearDown() throws SQLException {
    tearDownPresto();
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> databases() {
    Collection<Object[]> params = new ArrayList<>();

    for (String database : targetDatabases) {
      params.add(new Object[] {database});
    }
    return params;
  }
  
  private static void setupPresto() throws SQLException, VerdictDBException {
    String connectionString =
        String.format("jdbc:presto://%s/%s", PRESTO_HOST, PRESTO_CATALOG);
    Connection conn =
        DatabaseConnectionHelpers.setupPrestoForDataTypeTest(
            connectionString, PRESTO_USER, PRESTO_PASSWORD, SCHEMA_NAME, TABLE_NAME);
    VerdictConnection vc =
        new VerdictConnection(connectionString, PRESTO_USER, PRESTO_PASSWORD, options);
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictTempSchemaName()));
    conn.createStatement()
        .execute(
            String.format("CREATE SCHEMA IF NOT EXISTS %s", options.getVerdictMetaSchemaName()));
    connMap.put("presto", conn);
    vcMap.put("presto", vc);
    schemaMap.put("presto", "");
  }
  
  private static void tearDownPresto() throws SQLException {
    Connection conn = connMap.get("presto");
    Statement stmt = conn.createStatement();
    
    dropPrestoTablesInSchema(conn, SCHEMA_NAME);
    stmt.execute(String.format("DROP SCHEMA IF EXISTS \"%s\"", SCHEMA_NAME));
    
    dropPrestoTablesInSchema(conn, options.getVerdictMetaSchemaName());
    stmt.execute(
        String.format("DROP SCHEMA IF EXISTS \"%s\"", options.getVerdictMetaSchemaName()));
    
    dropPrestoTablesInSchema(conn, options.getVerdictTempSchemaName());
    stmt.execute(
        String.format("DROP SCHEMA IF EXISTS \"%s\"", options.getVerdictTempSchemaName()));
    conn.close();
  }
  
  private static void dropPrestoTablesInSchema(Connection conn, String schema_name) 
      throws SQLException {
    Statement stmt = conn.createStatement();
    List<String> tables = getPrestoTablesInSchema(conn, schema_name);
    for (String table_name : tables) {
      stmt.execute(String.format("drop table \"%s\".\"%s\"", schema_name, table_name));
    }
    stmt.close();
  }
  
  private static List<String> getPrestoTablesInSchema(Connection conn, String schema_name) 
      throws SQLException {
    List<String> tables = new ArrayList<>();
    Statement stmt = conn.createStatement();
    try {
      ResultSet result = stmt.executeQuery(String.format("show tables in \"%s\"", schema_name));
      while(result.next()) {
        tables.add(result.getString(1));
      }
      result.close();
    } catch (SQLException e) {
      
    } finally {
      stmt.close();
    }
    
    return tables;
  }
  
  @Test
  public void testDataType() throws SQLException, VerdictDBException {
    String sql = "";
    switch (database) {
      case "presto":
        sql = 
            String.format(
                "SELECT * FROM \"%s\".\"%s\" ORDER BY tinyintCol", SCHEMA_NAME, TABLE_NAME);
        break;
      default:
        fail(String.format("Database '%s' not supported.", database));
    }

    Statement jdbcStmt = connMap.get(database).createStatement();
    Statement vcStmt = vcMap.get(database).createStatement();

    ResultSet jdbcRs = jdbcStmt.executeQuery(sql);
    ResultSet vcRs = vcStmt.executeQuery(sql);

    int columnCount = jdbcRs.getMetaData().getColumnCount();
    while (jdbcRs.next() && vcRs.next()) {
      for (int i = 1; i <= columnCount; ++i) {
        String columnName = jdbcRs.getMetaData().getColumnName(i);
        Object theirs = jdbcRs.getObject(i);
        Object ours = vcRs.getObject(i);
        System.out.println(columnName + " >> " + theirs + " : " + ours);
        if (theirs instanceof byte[]) {
          assertTrue(Arrays.equals((byte[]) theirs, (byte[]) ours));
        } else if (theirs instanceof PgSQLXML) {
          PgSQLXML xml1 = (PgSQLXML) theirs;
          PgSQLXML xml2 = (PgSQLXML) ours;
          assertEquals(xml1.getString(), xml2.getString());
        } else {
          //          assertEquals(jdbcRs.getObject(i), vcRs.getObject(i));
          //          System.out.println(columnName + " >> " + theirs + " : " + ours);
          assertEquals(theirs, ours);
        }
      }
    }
  }

}
