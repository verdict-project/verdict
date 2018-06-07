package org.verdictdb.core.sql;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.parser.VerdictSQLLexer;
import org.verdictdb.parser.VerdictSQLParser;

public class SqlToRelation {

  MetaData meta;

  public SqlToRelation(MetaData meta) {
    this.meta = meta;
  }

  public static VerdictSQLParser parserOf(String text) {
    VerdictSQLLexer l = new VerdictSQLLexer(new ANTLRInputStream(text));
    VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
    return p;
  }

  public AbstractRelation ToRelation(String sql) {
    VerdictSQLParser p = parserOf(sql);
    RelationGen g = new RelationGen(meta);
    return g.visit(p.select_statement());
  }
}

