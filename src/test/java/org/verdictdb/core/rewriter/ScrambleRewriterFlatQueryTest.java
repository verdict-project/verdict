package org.verdictdb.core.rewriter;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.verdictdb.core.logical_query.SelectItem;
import org.verdictdb.core.logical_query.AbstractRelation;
import org.verdictdb.core.logical_query.AliasedColumn;
import org.verdictdb.core.logical_query.BaseColumn;
import org.verdictdb.core.logical_query.BaseTable;
import org.verdictdb.core.logical_query.ColumnOp;
import org.verdictdb.core.logical_query.SelectQueryOp;
import org.verdictdb.core.sql.RelationToSql;
import org.verdictdb.core.sql.syntax.HiveSyntax;
import org.verdictdb.exception.VerdictDbException;

public class ScrambleRewriterFlatQueryTest {
    
    int aggblockCount = 10;
    
    ScrambleMeta generateTestScrambleMeta() {
        ScrambleMeta meta = new ScrambleMeta();
        meta.insertScrumbleMetaEntry("myschema", "mytable",
                                     "verdictpartition", "verdictincprob", "verdictincprobblockdiff", "verdictsid",
                                     aggblockCount);
        return meta;
    }

    @Test
    public void testSelectSumBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
                Arrays.<SelectItem>asList(
                        new AliasedColumn(new ColumnOp("sum", new BaseColumn("t", "mycolumn1")), "a")),
                base);
        ScrambleMeta meta = generateTestScrambleMeta();
        ScrambleRewriter rewriter = new ScrambleRewriter(meta);
        List<AbstractRelation> rewritten = rewriter.rewrite(relation);
        
        for (int k = 0; k < aggblockCount; k++) {
            String expected = "select sum(`verdictalias1`.`verdictalias2`) as a, "
                    + "std(`verdictalias1`.`verdictalias2` * sqrt(`verdictalias1`.`verdictalias3`)) / "
                    + "sqrt(sum(`verdictalias1`.`verdictalias3`)) as std_a "
                    + "from ("
                    + "select sum(`t`.`mycolumn1` / (`t`.`verdictincprob` + (`t`.`verdictincprobblockdiff` * " + k + "))) as verdictalias2, "
                    + "sum(case 1 when `t`.`mycolumn1` is not null else 0 end) as verdictalias3 "
                    + "from `myschema`.`mytable` as t "
                    + "where `t`.`verdictpartition` = " + k + " "
                    + "group by `t`.`verdictsid`) as verdictalias1";
            RelationToSql relToSql = new RelationToSql(new HiveSyntax());
            String actual = relToSql.toSql(rewritten.get(k));
            assertEquals(expected, actual);
        }
    }
    
    @Test
    public void testSelectCountBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
                Arrays.<SelectItem>asList(new AliasedColumn(new ColumnOp("count"), "a")), base);
        ScrambleMeta meta = generateTestScrambleMeta();
        ScrambleRewriter rewriter = new ScrambleRewriter(meta);
        List<AbstractRelation> rewritten = rewriter.rewrite(relation);
        
        for (int k = 0; k < aggblockCount; k++) {
            String expected = "select sum(`verdictalias1`.`verdictalias2`) as a, "
                    + "std(`verdictalias1`.`verdictalias2` * sqrt(`verdictalias1`.`verdictalias3`)) / "
                    + "sqrt(sum(`verdictalias1`.`verdictalias3`)) as std_a "
                    + "from ("
                    + "select sum(1 / (`t`.`verdictincprob` + (`t`.`verdictincprobblockdiff` * " + k + "))) as verdictalias2, "
                    + "count(*) as verdictalias3 "
                    + "from `myschema`.`mytable` as t "
                    + "where `t`.`verdictpartition` = " + k + " "
                    + "group by `t`.`verdictsid`) as verdictalias1";
            RelationToSql relToSql = new RelationToSql(new HiveSyntax());
            String actual = relToSql.toSql(rewritten.get(k));
            assertEquals(expected, actual);
        }
    }
    
    @Test
    public void testSelectAvgBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
                Arrays.<SelectItem>asList(new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "mycolumn1")), "a")),
                base);
        ScrambleMeta meta = generateTestScrambleMeta();
        ScrambleRewriter rewriter = new ScrambleRewriter(meta);
        List<AbstractRelation> rewritten = rewriter.rewrite(relation);
        
        for (int k = 0; k < aggblockCount; k++) {
            String expected = "select sum(`verdictalias1`.`verdictalias2`) / sum(`verdictalias1`.`verdictalias3`) as a, "
                    + "std((`verdictalias1`.`verdictalias2` / `verdictalias1`.`verdictalias3`)"
                    + " * sqrt(`verdictalias1`.`verdictalias4`)) / "
                    + "sqrt(sum(`verdictalias1`.`verdictalias4`)) as std_a "
                    + "from ("
                    + "select sum(`t`.`mycolumn1` / (`t`.`verdictincprob` + (`t`.`verdictincprobblockdiff` * " + k + "))) as verdictalias2, "
                    + "sum((case 1 when `t`.`mycolumn1` is not null else 0 end) / "
                    + "(`t`.`verdictincprob` + (`t`.`verdictincprobblockdiff` * " + k + "))) as verdictalias3, "
                    + "sum(case 1 when `t`.`mycolumn1` is not null else 0 end) as verdictalias4 "
                    + "from `myschema`.`mytable` as t "
                    + "where `t`.`verdictpartition` = " + k + " "
                    + "group by `t`.`verdictsid`) as verdictalias1";
            RelationToSql relToSql = new RelationToSql(new HiveSyntax());
            String actual = relToSql.toSql(rewritten.get(k));
            assertEquals(expected, actual);
        }
    }

}
