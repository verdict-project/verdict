package edu.umich.verdict.util;

import edu.umich.verdict.exceptions.VerdictException;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

/**
 * Created by Dong Young Yoon on 4/11/18.
 */
public class VerdictAntlrErrorListener extends BaseErrorListener {
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
                            int charPositionInLine, String msg, RecognitionException e) {
        VerdictLogger.error(this, msg);
    }
}
