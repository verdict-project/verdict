package org.verdictdb.sql.syntax;

public class HiveSyntax implements SyntaxAbstract {

    @Override
    public String getQuoteString() {
        return "`";
    }

    @Override
    public void dropTable(String schema, String tablename) {
       
    }

    @Override
    public boolean doesSupportTablePartitioning() {
      return true;
    }

    @Override
    public String randFunction() {
      return "rand()";
    }

}
