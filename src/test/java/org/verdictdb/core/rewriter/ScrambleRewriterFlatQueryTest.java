package org.verdictdb.core.rewriter;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.verdictdb.core.logical_query.AbstractColumn;
import org.verdictdb.core.logical_query.AbstractRelation;
import org.verdictdb.core.logical_query.BaseColumn;
import org.verdictdb.core.logical_query.BaseTable;
import org.verdictdb.core.logical_query.ColumnOp;
import org.verdictdb.core.logical_query.SelectQueryOp;
import org.verdictdb.core.sql.RelationToSql;
import org.verdictdb.core.sql.syntax.HiveSyntax;
import org.verdictdb.exception.VerdictDbException;

public class ScrambleRewriterFlatQueryTest {
    
    ScrambleMeta generateTestScrambleMeta() {
        ScrambleMeta meta = new ScrambleMeta();
        List<String> partitionAttrValues = new ArrayList<>();
        for (int k = 0; k < 10; k++) {
            partitionAttrValues.add("part" + String.valueOf(k));
        }
        meta.insertScrumbleMetaEntry("myschema", "mytable", "verdictpartition", "verdictincprob", partitionAttrValues);
        return meta;
    }

    @Test
    public void testSelectSumBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
                Arrays.<AbstractColumn>asList(
                        new ColumnOp("sum", new BaseColumn("t", "mycolumn1"))),
                base);
        ScrambleMeta meta = generateTestScrambleMeta();
        ScrambleRewriter rewriter = new ScrambleRewriter(meta);
        List<AbstractRelation> rewritten = rewriter.rewrite(relation);
        
        for (int k = 0; k < 10; k++) {
            String expected = "select sum(`t`.`mycolumn1` / `t`.`verdictincprob`) "
                    + "from `myschema`.`mytable` "
                    + "where `t`.`verdictpartition` = part" + k;
            RelationToSql relToSql = new RelationToSql(new HiveSyntax());
            String actual = relToSql.toSql(rewritten.get(k));
            assertEquals(expected, actual);
        }
    }
    
    @Test
    public void testSelectCountBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
                Arrays.<AbstractColumn>asList(new ColumnOp("count")), base);
        ScrambleMeta meta = generateTestScrambleMeta();
        ScrambleRewriter rewriter = new ScrambleRewriter(meta);
        List<AbstractRelation> rewritten = rewriter.rewrite(relation);
        
        for (int k = 0; k < 10; k++) {
            String expected = "select sum(1 / `t`.`verdictincprob`) "
                    + "from `myschema`.`mytable` "
                    + "where `t`.`verdictpartition` = part" + k;
            RelationToSql relToSql = new RelationToSql(new HiveSyntax());
            String actual = relToSql.toSql(rewritten.get(k));
            assertEquals(expected, actual);
        }
    }
    
    @Test
    public void testSelectAvgBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
                Arrays.<AbstractColumn>asList(new ColumnOp("avg", new BaseColumn("t", "mycolumn1"))),
                base);
        ScrambleMeta meta = generateTestScrambleMeta();
        ScrambleRewriter rewriter = new ScrambleRewriter(meta);
        List<AbstractRelation> rewritten = rewriter.rewrite(relation);
        
        for (int k = 0; k < 10; k++) {
            String expected = "select sum(`t`.`mycolumn1` / `t`.`verdictincprob`), "
                    + "sum(case 1 when `t`.`mycolumn1` is not null else 0 end / `t`.`verdictincprob`) "
                    + "from `myschema`.`mytable` "
                    + "where `t`.`verdictpartition` = part" + k;
            RelationToSql relToSql = new RelationToSql(new HiveSyntax());
            String actual = relToSql.toSql(rewritten.get(k));
            assertEquals(expected, actual);
        }
    }

}
