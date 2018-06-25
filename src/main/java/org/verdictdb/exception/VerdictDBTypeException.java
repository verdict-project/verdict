package org.verdictdb.exception;

public class VerdictDBTypeException extends VerdictDBException {

    public VerdictDBTypeException(String message) {
        super(message);
    }
    
    public VerdictDBTypeException(Object obj) {
      super("Unexpected object type: " + obj.getClass().toString());
    }

}
