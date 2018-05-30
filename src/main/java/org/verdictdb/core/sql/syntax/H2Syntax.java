package org.verdictdb.core.sql.syntax;

public class H2Syntax implements SyntaxAbstract {

    public String getQuoteString() {
        return null;
    }

    public void dropTable(String schema, String tablename) {

    }

    @Override
    public boolean doesSupportTablePartitioning() {
      return false;
    }

    @Override
    public String randFunction() {
      return "rand()";
    }
}
