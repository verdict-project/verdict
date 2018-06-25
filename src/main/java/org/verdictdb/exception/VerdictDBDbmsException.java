package org.verdictdb.exception;

import java.sql.SQLException;

public class VerdictDBDbmsException extends VerdictDbException {

  public VerdictDBDbmsException(String message) {
    super(message);
  }
  
  public VerdictDBDbmsException(SQLException e) {
    this(e.getMessage());
  }

}
