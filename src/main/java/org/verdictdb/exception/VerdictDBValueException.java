package org.verdictdb.exception;

public class VerdictDBValueException extends VerdictDBException {

  private static final long serialVersionUID = -1901848579216388346L;

  public VerdictDBValueException(String message) {
    super(message);
  }

  public VerdictDBValueException(Exception e) {
    super(e.getMessage());
  }

}
