package org.verdictdb.coordinator;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;

public class ImpalaUniformScramblingCoordinatorTest {
  
  private static Connection impalaConn;
  
  private static Statement impalaStmt;

  private static final String IMPALA_HOST;

  private static final String IMPALA_DATABASE = "scrambling_coordinator_test";

  private static final String IMPALA_UESR = "";

  private static final String IMPALA_PASSWORD = "";
  
  static {
    IMPALA_HOST = System.getenv("VERDICTDB_TEST_IMPALA_HOST");
  }

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBDbmsException {
    String impalaConnectionString =
        String.format("jdbc:impala://%s:21050", IMPALA_HOST);
    impalaConn = 
        DatabaseConnectionHelpers.setupImpala(
            impalaConnectionString, IMPALA_UESR, IMPALA_PASSWORD, IMPALA_DATABASE);
//    impalaStmt = impalaConn.createStatement();
  }

  @Test
  public void sanityCheck() throws VerdictDBDbmsException {
    DbmsConnection dbmsConn = JdbcConnection.create(impalaConn);
//    System.out.println(dbmsConn.getColumns(IMPALA_DATABASE, "nation"));
//    System.out.println(dbmsConn.getColumns(IMPALA_DATABASE, "region"));
//    System.out.println(dbmsConn.getColumns(IMPALA_DATABASE, "part"));
//    System.out.println(dbmsConn.getColumns(IMPALA_DATABASE, "supplier"));
//    System.out.println(dbmsConn.getColumns(IMPALA_DATABASE, "partsupp"));
//    System.out.println(dbmsConn.getColumns(IMPALA_DATABASE, "customer"));
//    System.out.println(dbmsConn.getColumns(IMPALA_DATABASE, "orders"));
//    System.out.println(dbmsConn.getColumns(IMPALA_DATABASE, "lineitem"));
    
    assertEquals(5, dbmsConn.getColumns(IMPALA_DATABASE, "nation").size());
    assertEquals(4, dbmsConn.getColumns(IMPALA_DATABASE, "region").size());
    assertEquals(10, dbmsConn.getColumns(IMPALA_DATABASE, "part").size());
    assertEquals(8, dbmsConn.getColumns(IMPALA_DATABASE, "supplier").size());
    assertEquals(6, dbmsConn.getColumns(IMPALA_DATABASE, "partsupp").size());
    assertEquals(9, dbmsConn.getColumns(IMPALA_DATABASE, "customer").size());
    assertEquals(10, dbmsConn.getColumns(IMPALA_DATABASE, "orders").size());
    assertEquals(17, dbmsConn.getColumns(IMPALA_DATABASE, "lineitem").size());
  }

}
