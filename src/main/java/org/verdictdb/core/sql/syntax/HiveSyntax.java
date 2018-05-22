package org.verdictdb.core.sql.syntax;

public class HiveSyntax implements SyntaxAbstract {

    @Override
    public String getQuoteString() {
        return "`";
    }

    @Override
    public void dropTable(String schema, String tablename) {
       
    }

}
