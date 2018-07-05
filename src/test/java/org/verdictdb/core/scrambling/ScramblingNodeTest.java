package org.verdictdb.core.scrambling;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class ScramblingNodeTest {

  @Test
  public void test() {
    String newSchemaName = "newschema";
    String newTableName = "newtable";
    String oldSchemaName = "oldschema";
    String oldTableName = "oldtable";
    ScramblingMethod method = new UniformScramblingMethod();
    Map<String, String> options = new HashMap<>();
    options.put("blockColumnName", "blockcolumn");
    options.put("tierColumnName", "tiercolumn");
    options.put("blockCount", "10");
    
//    ScramblingNode node = ScramblingNode.create(newSchemaName, newTableName, oldSchemaName, oldTableName, method, options);
  }

}
