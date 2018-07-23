/*
 *    Copyright 2017 University of Michigan
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

package org.verdictdb.jdbc41;

import com.google.common.base.Joiner;
import org.verdictdb.exception.VerdictDBDbmsException;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Importat: If the name of this class changes, the change must be reflected in the service file
 * located at: src/main/resources/META-INF/services/java.sql.Driver
 *
 * @author Yongjoo Park
 */
public class Driver implements java.sql.Driver {

  static {
    try {
      DriverManager.registerDriver(new Driver());
    } catch (SQLException e) {
      System.err.println("Error occurred while registering VerdictDB driver:");
      System.err.println(e.getMessage());
    }
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (acceptsURL(url)) {
      String newUrl = url;
      try {
        String[] tokens = url.split(":");
        if (tokens.length >= 2
            && (tokens[1].equalsIgnoreCase("verdict") || tokens[1].equalsIgnoreCase("verdictdb"))) {
          List<String> newTokens = new ArrayList<>();
          for (int i = 0; i < tokens.length; ++i) {
            if (i != 1) newTokens.add(tokens[i]);
          }
          newUrl = Joiner.on(":").join(newTokens);
        }
        Connection verdictConnection = new org.verdictdb.jdbc41.VerdictConnection(newUrl, info);
        System.out.println("VerdictConnection has been created: " + verdictConnection);
        return verdictConnection;
      } catch (VerdictDBDbmsException e) {
        e.printStackTrace();
        throw new SQLException(e.getMessage());
      }
    }
    return null;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    String[] tokens = url.split(":");
    if (tokens.length >= 2
        && (tokens[1].equalsIgnoreCase("verdict") || tokens[1].equalsIgnoreCase("verdictdb"))) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 5;
  }

  @Override
  public boolean jdbcCompliant() {
    return true;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }
}
