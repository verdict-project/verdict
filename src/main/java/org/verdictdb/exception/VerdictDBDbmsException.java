package org.verdictdb.exception;

import java.sql.SQLException;

public class VerdictDBDbmsException extends VerdictDBException {

  private static final long serialVersionUID = 2290359426146594968L;

  public VerdictDBDbmsException(String message) {
    super(message);
  }
  
  public VerdictDBDbmsException(SQLException e) {
    this(e.getMessage());
  }

}
