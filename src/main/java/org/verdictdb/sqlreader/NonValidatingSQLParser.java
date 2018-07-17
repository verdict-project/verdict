package org.verdictdb.sqlreader;

import java.util.List;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.parser.VerdictSQLLexer;
import org.verdictdb.parser.VerdictSQLParser;

public class NonValidatingSQLParser {

//  MetaData meta;

  public NonValidatingSQLParser() {}
//    this.meta = meta;
//  }

  public static VerdictSQLParser parserOf(String text) {
    VerdictDBErrorListener verdictDBErrorListener = new VerdictDBErrorListener();
    VerdictSQLLexer l = new VerdictSQLLexer(new ANTLRInputStream(text));
    l.removeErrorListeners();
    l.addErrorListener(verdictDBErrorListener);

    VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
    p.removeErrorListeners();
    p.addErrorListener(verdictDBErrorListener);
    return p;
  }

  public AbstractRelation toRelation(String sql) {
    VerdictSQLParser p = parserOf(sql);
    RelationGen g = new RelationGen();
    return g.visit(p.select_statement());
  }
  
  /**
   * 
   * @param rel
   * @return  Pairs of (schemaName, tableName) that appear in the argument.
   */
  public static List<Pair<String, String>> extractInvolvedTables(AbstractRelation rel) {
    return null;
  }
}

