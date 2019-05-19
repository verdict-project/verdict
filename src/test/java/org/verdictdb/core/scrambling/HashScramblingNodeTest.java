package org.verdictdb.core.scrambling;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlwriter.QueryToSql;

public class HashScramblingNodeTest {

  @Test
  public void testHashScramblingMethodStatisticsWithPredicate() throws VerdictDBException {
    String oldSchemaName = "oldschema";
    String oldTableName = "oldtable";
    int blockSize = 2;    // 2 blocks will be created (since the total is 4 due to a predicate)
    ScramblingMethod method = new HashScramblingMethod(blockSize, 100, 1.0, "id");
    Map<String, String> options = new HashMap<>();
    options.put("tierColumnName", "tiercolumn");
    options.put("blockColumnName", "blockcolumn");
    UnnamedColumn predicate = ColumnOp.greaterequal(
        new BaseColumn("t", "id"),  
        ConstantColumn.valueOf(2));
    
    List<ExecutableNodeBase> statNodes = 
        method.getStatisticsNode(oldSchemaName, oldTableName, predicate, null, null, null);
    ExecutableNodeBase statNode = statNodes.get(0);
    
    // set empty tokens
    List<ExecutionInfoToken> tokens = new ArrayList<>();
    tokens.add(new ExecutionInfoToken());
    
    SqlConvertible query = statNode.createQuery(tokens);
    String sql = QueryToSql.convert(new MysqlSyntax(), query);
    assertEquals(
        "select count(*) as `verdictdbtotalcount` " +
        "from `oldschema`.`oldtable` as t " +
        "where t.`id` >= 2",
        sql);
  }

}
