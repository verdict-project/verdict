package org.verdictdb.core.sql;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.parser.VerdictSQLLexer;
import org.verdictdb.parser.VerdictSQLParser;

public class NonValidatingSQLParser {

//  MetaData meta;

  public NonValidatingSQLParser() {}
//    this.meta = meta;
//  }

  public static VerdictSQLParser parserOf(String text) {
    VerdictSQLLexer l = new VerdictSQLLexer(new ANTLRInputStream(text));
    VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
    return p;
  }

  public AbstractRelation toRelation(String sql) {
    VerdictSQLParser p = parserOf(sql);
    RelationGen g = new RelationGen();
    return g.visit(p.select_statement());
  }
}

