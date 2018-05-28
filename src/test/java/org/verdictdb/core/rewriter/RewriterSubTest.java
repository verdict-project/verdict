package org.verdictdb.core.rewriter;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.verdictdb.core.logical_query.AbstractRelation;
import org.verdictdb.core.logical_query.AliasedColumn;
import org.verdictdb.core.logical_query.AsteriskColumn;
import org.verdictdb.core.logical_query.BaseColumn;
import org.verdictdb.core.logical_query.BaseTable;
import org.verdictdb.core.logical_query.ColumnOp;
import org.verdictdb.core.logical_query.SelectItem;
import org.verdictdb.core.logical_query.SelectQueryOp;
import org.verdictdb.core.sql.RelationToSql;
import org.verdictdb.core.sql.syntax.HiveSyntax;
import org.verdictdb.exception.VerdictDbException;

public class RewriterSubTest {

    int aggblockCount = 10;

    ScrambleMeta generateTestScrambleMeta() {
        ScrambleMeta meta = new ScrambleMeta();
        meta.insertScrambleMetaEntry("myschema", "mytable",
                "verdictaggblock", "verdictincprob", "verdictincprobblockdiff", "verdictsid",
                aggblockCount);
        return meta;
    }
    
    @Test
    public void testScrambledBaseTableRewriting() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        SelectQueryOp original = SelectQueryOp.getSelectQueryOp(Arrays.<SelectItem>asList(new AsteriskColumn()), base);
        ScrambleMeta meta = generateTestScrambleMeta();
        ScrambleRewriter rewriter = new ScrambleRewriter(meta);
        List<AbstractRelation> rewritten = rewriter.rewrite(original);
        for (int k = 0; k < rewritten.size(); k++) {
            String expected = "select * from `myschema`.`mytable` as t where `t`.`verdictaggblock` = " + k;
            RelationToSql relToSql = new RelationToSql(new HiveSyntax());
            String actual = relToSql.toSql(rewritten.get(k));
            assertEquals(expected, actual);
        }
    }
    
    @Test
    public void testRegularBaseTableRewriting() throws VerdictDbException {
        BaseTable base = new BaseTable("myschema", "mytable", "t");
        ScrambleMeta meta = new ScrambleMeta();
        ScrambleRewriter rewriter = new ScrambleRewriter(meta);
        List<AbstractRelation> rewritten = rewriter.rewrite(base);
        assertEquals(1, rewritten.size());
        
        String expected = "select * from `myschema`.`mytable` as t";
        RelationToSql relToSql = new RelationToSql(new HiveSyntax());
        String actual = relToSql.toSql(rewritten.get(0));
        assertEquals(expected, actual);
    }

}
