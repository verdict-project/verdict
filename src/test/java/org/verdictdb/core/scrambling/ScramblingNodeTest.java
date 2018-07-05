package org.verdictdb.core.scrambling;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;

public class ScramblingNodeTest {

  @Test
  public void testScramblingNodeCreation() throws VerdictDBException {
    String newSchemaName = "newschema";
    String newTableName = "newtable";
    String oldSchemaName = "oldschema";
    String oldTableName = "oldtable";
    ScramblingMethod method = new UniformScramblingMethod();
    Map<String, String> options = new HashMap<>();
    options.put("blockColumnName", "blockcolumn");
    options.put("tierColumnName", "tiercolumn");
    options.put("blockCount", "10");
    
    ScramblingNode node = ScramblingNode.create(newSchemaName, newTableName, oldSchemaName, oldTableName, method, options);
    
    List<ExecutionInfoToken> tokens = new ArrayList<>();
    ExecutionInfoToken e = new ExecutionInfoToken();
    e.setKeyValue("schemaName", newSchemaName);
    e.setKeyValue("tableName", newTableName);
    tokens.add(e);
//    SqlConvertible query = node.createQuery(tokens);
//    QueryToSql.convert(query, new MysqlSyntax());)
  }

}
