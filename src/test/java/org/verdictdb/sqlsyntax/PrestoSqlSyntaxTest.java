package org.verdictdb.sqlsyntax;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.verdictdb.category.PrestoTests;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;

@Category(PrestoTests.class)
public class PrestoSqlSyntaxTest {

  @Test
  public void testPrestoSyntaxInference() throws SQLException, VerdictDBDbmsException {
    final String PRESTO_HOST = System.getenv("VERDICTDB_TEST_PRESTO_HOST");
    final String PRESTO_CATALOG = System.getenv("VERDICTDB_TEST_PRESTO_CATALOG");
    final String PRESTO_USER = System.getenv("VERDICTDB_TEST_PRESTO_USER");
    final String PRESTO_PASSWORD = System.getenv("VERDICTDB_TEST_PRESTO_PASSWORD");
    
    String prestoConnectionString =
        String.format("jdbc:presto://%s/%s/default", PRESTO_HOST, PRESTO_CATALOG);
    
    Connection conn = 
        DriverManager.getConnection(prestoConnectionString, PRESTO_USER, PRESTO_PASSWORD);

    SqlSyntax expected = new PrestoHiveSyntax();
    SqlSyntax actual = JdbcConnection.create(conn).getSyntax();
    assertEquals(expected, actual);
  }

}
