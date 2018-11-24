/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.CachedDbmsConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;

/** Created by Dong Young Yoon on 8/9/18. */
public class VerdictContextInitTest {

  private static final String MYSQL_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_USER = "root";

  private static final String MYSQL_PASSWORD = "";
  
  private static final String SCHEMA_NAME =
      "verdictcontext_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
  
  private static final String VERDICT_META_SCHEMA =
      "verdictdbmetaschema_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
  private static final String VERDICT_TEMP_SCHEMA =
      "verdictdbtempschema_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
  
  static VerdictContext vc;
  
  @BeforeClass
  public static void setupMysql() throws VerdictDBException, SQLException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    String vcMysqlConnectionString =
        String.format(
            "jdbc:mysql://%s?autoReconnect=true&useSSL=false&"
                + "verdictdbmetaschema=%s&verdictdbtempschema=%s",
            MYSQL_HOST, VERDICT_META_SCHEMA, VERDICT_TEMP_SCHEMA);
    
    DatabaseConnectionHelpers.setupMySql(
        mysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD, SCHEMA_NAME);
    vc = VerdictContext.fromConnectionString(vcMysqlConnectionString, MYSQL_USER, MYSQL_PASSWORD);
    
//    conn.setCatalog(SCHEMA_NAME);
    vc.setDefaultSchema(SCHEMA_NAME);
  }
  
  // intend to see if the metadata is retrieved multiple times.
  @Test
  public void dbmsCacheTest() throws VerdictDBException {
    vc.sql(String.format("select count(*) from %s.orders", SCHEMA_NAME));
    
    CachedDbmsConnection conn = (CachedDbmsConnection) vc.getConnection();
    conn.close();
    conn.getTables(SCHEMA_NAME);
    
//    vc.sql(String.format("select count(*) from %s.orders", SCHEMA_NAME));
  }

  @Test(expected = VerdictDBException.class)
  public void initFromUnsupportedConnectionStringTest() throws SQLException, VerdictDBException {
    String url =
        String.format(
            "jdbc:unsupporteddb://%s?autoReconnect=true&useSSL=false&"
                + "verdictdbmetaschema=mymeta&verdictdbtempschema=mytemp",
            MYSQL_HOST);
    VerdictContext context = VerdictContext.fromConnectionString(url, MYSQL_USER, MYSQL_PASSWORD);
    VerdictOption options = context.getOptions();

    assertEquals("mymeta", options.getVerdictMetaSchemaName());
    assertEquals("mytemp", options.getVerdictTempSchemaName());
  }

  @Test
  public void initFromConnectionStringTest1() throws SQLException, VerdictDBException {
    String url =
        String.format(
            "jdbc:mysql://%s?autoReconnect=true&useSSL=false&"
                + "verdictdbmetaschema=mymeta&verdictdbtempschema=mytemp",
            MYSQL_HOST);
    VerdictContext context = VerdictContext.fromConnectionString(url, MYSQL_USER, MYSQL_PASSWORD);
    VerdictOption options = context.getOptions();

    assertEquals("mymeta", options.getVerdictMetaSchemaName());
    assertEquals("mytemp", options.getVerdictTempSchemaName());
  }

  @Test
  public void initFromConnectionStringTest2() throws SQLException, VerdictDBException {
    String url =
        String.format(
            "jdbc:mysql://%s?autoReconnect=true&useSSL=false&user=root&password=", MYSQL_HOST);
    Properties prop = new Properties();
    prop.setProperty("verdictdbmetaschema", "mymeta");
    prop.setProperty("verdictdbtempschema", "mytemp");
    VerdictContext context = VerdictContext.fromConnectionString(url, prop);
    VerdictOption options = context.getOptions();

    assertEquals("mymeta", options.getVerdictMetaSchemaName());
    assertEquals("mytemp", options.getVerdictTempSchemaName());
  }
}
