package org.verdictdb.exception;

public class UnexpectedTypeException extends VerdictDbException {

    public UnexpectedTypeException(String message) {
        super(message);
    }
    
    public UnexpectedTypeException(Object obj) {
      super("Unexpected object type: " + obj.getClass().toString());
    }

}
