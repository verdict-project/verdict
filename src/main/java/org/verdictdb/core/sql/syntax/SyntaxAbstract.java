package org.verdictdb.core.sql.syntax;

public interface SyntaxAbstract {

    public String getQuoteString();

    public void dropTable(String schema, String tablename);
}
