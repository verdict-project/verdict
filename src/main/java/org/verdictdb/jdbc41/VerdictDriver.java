package org.verdictdb.jdbc41;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import org.verdictdb.exception.VerdictDBDbmsException;

public class VerdictDriver implements java.sql.Driver {

  static {
    try {
      DriverManager.registerDriver(new VerdictDriver());
    } catch (SQLException e) {
      System.err.println("Error occurred while registering VerdictDB driver:");
      System.err.println(e.getMessage());
    }
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (acceptsURL(url)) {
      try {
        return new org.verdictdb.jdbc41.VerdictConnection(url, info);
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
    if (tokens.length >= 2 && 
        (tokens[1].equalsIgnoreCase("verdict") || tokens[1].equalsIgnoreCase("verdictdb"))) {
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