package org.verdictdb.exception;

public class VerdictDBTypeException extends VerdictDBException {

  private static final long serialVersionUID = -8748835227050280206L;

  public VerdictDBTypeException(String message) {
    super(message);
  }

  public VerdictDBTypeException(Object obj) {
    super("Unexpected object type: " + obj.getClass().toString());
  }

}
