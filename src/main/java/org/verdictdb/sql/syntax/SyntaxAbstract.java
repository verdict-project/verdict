package org.verdictdb.sql.syntax;

public interface SyntaxAbstract {

    public String getQuoteString();

    public void dropTable(String schema, String tablename);
    
    public boolean doesSupportTablePartitioning();
    
    public String randFunction();
}
