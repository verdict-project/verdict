package org.verdictdb.core.scramble;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.verdictdb.core.logical_query.CreateTableAsSelect;
import org.verdictdb.core.logical_query.SelectQueryOp;
import org.verdictdb.core.rewriter.ScrambleMetaForTable;
import org.verdictdb.core.sql.CreateTableToSql;
import org.verdictdb.core.sql.SelectQueryToSql;
import org.verdictdb.core.sql.syntax.HiveSyntax;
import org.verdictdb.exception.VerdictDbException;

public class UniformScramblerTest {

  @Test
  public void testSelectQuery() throws VerdictDbException {
    String originalSchema = "originalschema";
    String originalTable = "originalschema";
    String newSchema = "newschema";
    String newTable  = "newschema";
    int aggBlockCount = 10;
    UniformScrambler scrambler =
        new UniformScrambler(originalSchema, originalTable, newSchema, newTable, aggBlockCount);
    SelectQueryOp scramblingQuery = scrambler.scramblingQuery();
    
    ScrambleMetaForTable meta = scrambler.generateMeta();
    meta.getAggregationBlockColumn();
    
    String expected = "select *"
        + String.format(", floor(rand() * %d) as %s", aggBlockCount, meta.getAggregationBlockColumn())
        + String.format(", 0.1 as %s", meta.getInclusionProbabilityColumn())
        + String.format(", 0.1 as %s", meta.getInclusionProbabilityBlockDifferenceColumn())
        + String.format(", floor(rand() * 100) as %s", meta.getSubsampleColumn())
        + String.format(" from `%s`.`%s`", originalSchema, originalTable);
    SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
    String actual = relToSql.toSql(scramblingQuery);
    assertEquals(expected, actual);
  }
  
  @Test
  public void testCreateTableQuery() throws VerdictDbException {
    String originalSchema = "originalschema";
    String originalTable = "originalschema";
    String newSchema = "newschema";
    String newTable  = "newschema";
    int aggBlockCount = 10;
    UniformScrambler scrambler =
        new UniformScrambler(originalSchema, originalTable, newSchema, newTable, aggBlockCount);
    CreateTableAsSelect createQuery = scrambler.scrambledTableCreationQuery();
    
    ScrambleMetaForTable meta = scrambler.generateMeta();
    meta.getAggregationBlockColumn();
    
    String expected = String.format("create table `%s`.`%s` ", newSchema, newTable)
        + String.format("partitioned by (`%s`) ", meta.getAggregationBlockColumn())
        + "as select *"
        + String.format(", floor(rand() * %d) as %s", aggBlockCount, meta.getAggregationBlockColumn())
        + String.format(", 0.1 as %s", meta.getInclusionProbabilityColumn())
        + String.format(", 0.1 as %s", meta.getInclusionProbabilityBlockDifferenceColumn())
        + String.format(", floor(rand() * 100) as %s", meta.getSubsampleColumn())
        + String.format(" from `%s`.`%s`", originalSchema, originalTable);
    CreateTableToSql createToSql = new CreateTableToSql(new HiveSyntax());
    String actual = createToSql.toSql(createQuery);
    assertEquals(expected, actual);
  }

}
