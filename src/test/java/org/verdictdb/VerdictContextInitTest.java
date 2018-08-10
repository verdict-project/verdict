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

import org.junit.Test;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.exception.VerdictDBDbmsException;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

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

  @Test
  public void initFromConnectionStringTest1() throws SQLException, VerdictDBDbmsException {
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
  public void initFromConnectionStringTest2() throws SQLException, VerdictDBDbmsException {
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
