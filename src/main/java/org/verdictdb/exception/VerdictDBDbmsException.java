package org.verdictdb.exception;

import java.sql.SQLException;

public class VerdictDBDbmsException extends VerdictDBException {

  public VerdictDBDbmsException(String message) {
    super(message);
  }
  
  public VerdictDBDbmsException(SQLException e) {
    this(e.getMessage());
  }

}
