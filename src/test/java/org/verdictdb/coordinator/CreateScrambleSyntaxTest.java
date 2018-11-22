package org.verdictdb.coordinator;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;

import org.junit.Test;
import org.verdictdb.coordinator.ExecutionContext.QueryType;
import org.verdictdb.exception.VerdictDBException;

public class CreateScrambleSyntaxTest {

  @Test
  public void hashScrambleSyntax1Test() throws SQLException, VerdictDBException {
    String createScrambleSql =
        "CREATE SCRAMBLE tpch.lineitem_hash_scramble "
            + "FROM tpch.lineitem "
            + "METHOD 'hash' "
            + "HASHCOLUMN l_orderkey";
    QueryType type = ExecutionContext.identifyQueryType(createScrambleSql);
    assertEquals(ExecutionContext.QueryType.scrambling, type);
  }
  
  @Test
  public void hashScrambleSyntax2Test() throws SQLException, VerdictDBException {
    String createScrambleSql =
        "CREATE SCRAMBLE tpch.lineitem_hash_scramble "
            + "FROM tpch.lineitem "
            + "METHOD hash "
            + "HASHCOLUMN `l_orderkey`";
    QueryType type = ExecutionContext.identifyQueryType(createScrambleSql);
    assertEquals(ExecutionContext.QueryType.scrambling, type);
  }
  
}
