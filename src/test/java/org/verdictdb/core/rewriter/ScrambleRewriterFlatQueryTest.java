package org.verdictdb.core.rewriter;

import static org.junit.Assert.*;

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

    @Test
    public void testSelectSumBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
                Arrays.<AbstractColumn>asList(
                        new ColumnOp("sum", new BaseColumn("t", "mycolumn1"))),
                base);
        ScrambleMeta meta = new ScrambleMeta();
        meta.insertScrumbleMetaEntry("myschema", "mytable", "verdictpartition", "verdictincprob", 10);
        ScrambleRewriter rewriter = new ScrambleRewriter(meta);
        List<AbstractRelation> rewritten = rewriter.rewrite(relation);
        
        for (int k = 0; k < 10; k++) {
            String expected = "select sum(`t`.`mycolumn1` / `t`.`verdictincprob`) "
                    + "from `myschema`.`mytable` "
                    + "where `t`.`verdictpartition` = " + k;
            RelationToSql relToSql = new RelationToSql(new HiveSyntax());
            String actual = relToSql.toSql(rewritten.get(k));
            assertEquals(expected, actual);
        }
    }
    
    @Test
    public void testSelectCountBaseTable() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
                Arrays.<AbstractColumn>asList(new ColumnOp("count")),
                base);
        ScrambleMeta meta = new ScrambleMeta();
        meta.insertScrumbleMetaEntry("myschema", "mytable", "verdictpartition", "verdictincprob", 10);
        ScrambleRewriter rewriter = new ScrambleRewriter(meta);
        List<AbstractRelation> rewritten = rewriter.rewrite(relation);
        
        for (int k = 0; k < 10; k++) {
            String expected = "select sum(1 / `t`.`verdictincprob`) "
                    + "from `myschema`.`mytable` "
                    + "where `t`.`verdictpartition` = " + k;
            RelationToSql relToSql = new RelationToSql(new HiveSyntax());
            String actual = relToSql.toSql(rewritten.get(k));
            assertEquals(expected, actual);
        }
    }

}
