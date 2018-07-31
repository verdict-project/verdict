package org.verdictdb.core.querying;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.execplan.ExecutableNodeRunner;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.H2Syntax;

public class CreateTableAsSelectNodeTest {
  
  static String originalSchema = "originalschema";

  static String originalTable = "originalschema";

  static String newSchema = "newschema";

  static String newTable  = "newtable";

  static int aggblockCount = 2;

  // H2
  static DbmsConnection h2conn;
  
  // Redshift
  private static final String REDSHIFT_HOST;

  private static final String REDSHIFT_USER;

  private static final String REDSHIFT_PASSWORD;
  
  private static final String REDSHIFT_DATABASE = "dev";

  private static final String REDSHIFT_SCHEMA = "create_table_as_select_test";
  
  private static final String REDSHIFT_TEST_TABLE = "mytable";
  
  static {
    REDSHIFT_HOST = System.getenv("VERDICTDB_TEST_REDSHIFT_ENDPOINT");
    REDSHIFT_USER = System.getenv("VERDICTDB_TEST_REDSHIFT_USER");
    REDSHIFT_PASSWORD = System.getenv("VERDICTDB_TEST_REDSHIFT_PASSWORD");
  }
  
  static Connection redshiftConn;
  
  static DbmsConnection redshiftVerdictConn;

  @BeforeClass
  public static void setupH2() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:createasselecttest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    h2conn = new JdbcConnection(DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD), new H2Syntax());
    h2conn.execute(String.format("CREATE SCHEMA \"%s\"", originalSchema));
    h2conn.execute(String.format("CREATE SCHEMA \"%s\"", newSchema));
    populateData(h2conn, originalSchema, originalTable);
  }
  
  @BeforeClass
  public static void setupRedshift() throws SQLException, VerdictDBDbmsException {
    String connectionString =
        String.format("jdbc:redshift://%s/%s", REDSHIFT_HOST, REDSHIFT_DATABASE);
    redshiftConn = DriverManager.getConnection(connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
    redshiftVerdictConn = JdbcConnection.create(redshiftConn);
    
    redshiftVerdictConn.execute(String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", REDSHIFT_SCHEMA));
    redshiftVerdictConn.execute(String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", REDSHIFT_SCHEMA));
    
    redshiftVerdictConn.execute(
        String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double precision)", 
            REDSHIFT_SCHEMA, REDSHIFT_TEST_TABLE));
    for (int i = 0; i < 10; i++) {
      redshiftVerdictConn.execute(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          REDSHIFT_SCHEMA, REDSHIFT_TEST_TABLE, i, (double) i+1));
    }
  }
  
  @AfterClass
  public static void tearDown() {
    
  }
  
  @Test
  public void testRedshiftCreatePartitionedTableAsSelect() throws VerdictDBDbmsException {
    // create a partitioned table (using SORTKEY)
    final String partitionedTable = "newtable";
    CreateTableAsSelectNode node = new CreateTableAsSelectNode(
        new IdCreator() {
          @Override
          public String generateAliasName() {
            return "t";
          }
  
          @Override
          public String generateAliasName(String keyword) {
            return "t";
          }
  
          @Override
          public int generateSerialNumber() {
            return 0;
          }
  
          @Override
          public Pair<String, String> generateTempTableName() {
            return Pair.of(REDSHIFT_SCHEMA, partitionedTable);
          }
          
        },
        SelectQuery.create(new AsteriskColumn(), new BaseTable(REDSHIFT_SCHEMA, REDSHIFT_TEST_TABLE)));
    node.addPartitionColumn("id");
    
    ExecutableNodeRunner runner = new ExecutableNodeRunner(redshiftVerdictConn, node);
    runner.run();
    
    // tests
    List<String> partitions = redshiftVerdictConn.getPartitionColumns(REDSHIFT_SCHEMA, partitionedTable);
    assertEquals(1, partitions.size());
    assertEquals("id", partitions.get(0));
  }

  @Test
  public void testExecuteNode() throws VerdictDBException {
    BaseTable base = new BaseTable(originalSchema, originalTable, "t");
    SelectQuery query = SelectQuery.create(Arrays.<SelectItem>asList(new AsteriskColumn()), base);
    ExecutableNodeBase root = CreateTableAsSelectNode.create(QueryExecutionPlanFactory.create("newschema"), query);
//    ExecutionInfoToken token = new ExecutionInfoToken();
    ExecutionInfoToken newTableName = 
        ExecutableNodeRunner.execute(h2conn, root);
    
    String schemaName = (String) newTableName.getValue("schemaName");
    String tableName = (String) newTableName.getValue("tableName");
    h2conn.execute(String.format("DROP TABLE \"%s\".\"%s\"", schemaName, tableName));
  }

  static void populateData(DbmsConnection conn, String schemaName, String tableName) throws VerdictDBDbmsException {
    conn.execute(String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)", schemaName, tableName));
    for (int i = 0; i < 2; i++) {
      conn.execute(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          schemaName, tableName, i, (double) i+1));
    }
  }
  
}
