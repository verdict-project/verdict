package org.verdictdb.jdbc41;

import static org.junit.Assert.*;

import groovy.sql.Sql;
import org.junit.Test;
import org.verdictdb.exception.VerdictDBDbmsException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by Dong Young Yoon on 7/18/18.
 */
public class JdbcConnectionTestForMySql {

  static Connection conn;

  private static final String MYSQL_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_DATABASE = "test";

  private static final String MYSQL_USER = "root";

  private static final String MYSQL_PASSWORD = "";

  private static String connectionString = String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false",
      MYSQL_HOST, MYSQL_DATABASE);

  @Test
  public void connectWithUserPasswordTest() throws VerdictDBDbmsException, SQLException {
    VerdictConnection vc = new VerdictConnection(connectionString, MYSQL_USER, MYSQL_PASSWORD);
    Statement stmt = vc.createStatement();
    assertNotNull(stmt);
    vc.close();
  }

  @Test
  public void connectWithPropertiesTest() throws VerdictDBDbmsException, SQLException {
    Properties prop = new Properties();
    prop.setProperty("user", MYSQL_USER);
    prop.setProperty("password", MYSQL_PASSWORD);
    VerdictConnection vc = new VerdictConnection(connectionString, prop);
    Statement stmt = vc.createStatement();
    assertNotNull(stmt);
    vc.close();
  }
}
