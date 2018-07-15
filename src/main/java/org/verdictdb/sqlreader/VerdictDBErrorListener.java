package org.verdictdb.sqlreader;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/**
 *
 * Error Listener of Antlr parser. Throw Runtime error once parser fails.
 */
public class VerdictDBErrorListener extends BaseErrorListener {

  @Override
  public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
                          String msg, RecognitionException e ) {
    // method arguments should be used for more detailed report
    throw new RuntimeException(String.format("syntax error occurred:%s", msg));
  }
}
