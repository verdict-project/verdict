package org.verdictdb.core.rewriter;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.verdictdb.core.logical_query.AbstractRelation;
import org.verdictdb.core.logical_query.AliasReference;
import org.verdictdb.core.logical_query.AliasedColumn;
import org.verdictdb.core.logical_query.BaseColumn;
import org.verdictdb.core.logical_query.BaseTable;
import org.verdictdb.core.logical_query.ColumnOp;
import org.verdictdb.core.logical_query.SelectItem;
import org.verdictdb.core.logical_query.SelectQueryOp;
import org.verdictdb.core.sql.SelectQueryToSql;
import org.verdictdb.core.sql.syntax.HiveSyntax;
import org.verdictdb.exception.VerdictDbException;

public class AggQueryRewriterTest {

  int aggblockCount = 10;

  ScrambleMeta generateTestScrambleMeta() {
    ScrambleMeta meta = new ScrambleMeta();
    meta.insertScrambleMetaEntry("myschema", "mytable",
        "verdictpartition", "verdictincprob", "verdictincprobblockdiff", "verdictsid",
        aggblockCount);
    return meta;
  }

  @Test
  public void testSelectSumBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    String aliasName = "a";
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("t", "mycolumn1")), aliasName)),
        base);
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewritten = rewriter.rewrite(relation);
    
    String aliasForSumEstimate = AliasRenamingRules.sumEstimateAliasName(aliasName);
    String aliasForSumScaledSubsum = AliasRenamingRules.sumScaledSumAliasName(aliasName);
    String aliasForSumSquaredScaledSubsum = AliasRenamingRules.sumSquaredScaledSumAliasName(aliasName);
    String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
    String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

    for (int k = 0; k < aggblockCount; k++) {
      String expected = "select sum(`verdictalias5`.`verdictalias6`) as " + aliasForSumEstimate + ", "
          + "sum(`verdictalias5`.`verdictalias6` * "
          + "`verdictalias5`.`verdictalias7`) as " + aliasForSumScaledSubsum + ", "
          + "sum((`verdictalias5`.`verdictalias6` * `verdictalias5`.`verdictalias6`) * "
          + "`verdictalias5`.`verdictalias7`) as " + aliasForSumSquaredScaledSubsum + ", "
          + "count(*) as " + aliasForCountSubsample + ", "
          + "sum(`verdictalias5`.`verdictalias7`) as " + aliasForSumSubsampleSize + " "
          + "from ("
          + "select sum(`verdictalias1`.`mycolumn1` / "
          + "(`verdictalias1`.`verdictalias2` + (`verdictalias1`.`verdictalias3` * " + k
          + "))) as verdictalias6, "
          + "sum(case 1 when `verdictalias1`.`mycolumn1` is not null else 0 end) as verdictalias7 "
          + "from (select *, `t`.`verdictincprob` as verdictalias2, "
          + "`t`.`verdictincprobblockdiff` as verdictalias3, "
          + "`t`.`verdictsid` as verdictalias4 "
          + "from `myschema`.`mytable` as t "
          + "where `t`.`verdictpartition` = " + k + ") as verdictalias1 "
          + "group by `verdictalias1`.`verdictalias4`) as verdictalias5";
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testSelectCountBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    String aliasName = "a";
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(new AliasedColumn(new ColumnOp("count"), aliasName)), base);
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewritten = rewriter.rewrite(relation);
    
    String aliasForCountEstimate = AliasRenamingRules.countEstimateAliasName(aliasName);
    String aliasForSumScaledSubcount = AliasRenamingRules.sumScaledCountAliasName(aliasName);
    String aliasForSumSquaredScaledSubcount = AliasRenamingRules.sumSquaredScaledCountAliasName(aliasName);
    String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
    String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

    for (int k = 0; k < aggblockCount; k++) {
      String expected = "select sum(`verdictalias5`.`verdictalias6`) as " + aliasForCountEstimate + ", "
          + "sum(`verdictalias5`.`verdictalias6` * "
          + "`verdictalias5`.`verdictalias7`) as " + aliasForSumScaledSubcount + ", "
          + "sum((`verdictalias5`.`verdictalias6` * `verdictalias5`.`verdictalias6`) * "
          + "`verdictalias5`.`verdictalias7`) as " + aliasForSumSquaredScaledSubcount + ", "
          + "count(*) as " + aliasForCountSubsample + ", "
          + "sum(`verdictalias5`.`verdictalias7`) as " + aliasForSumSubsampleSize + " "
          + "from ("
          + "select sum(1 / "
          + "(`verdictalias1`.`verdictalias2` + (`verdictalias1`.`verdictalias3` * " + k
          + "))) as verdictalias6, "
          + "count(*) as verdictalias7 "
          + "from (select *, `t`.`verdictincprob` as verdictalias2, "
          + "`t`.`verdictincprobblockdiff` as verdictalias3, "
          + "`t`.`verdictsid` as verdictalias4 "
          + "from `myschema`.`mytable` as t "
          + "where `t`.`verdictpartition` = " + k + ") as verdictalias1 "
          + "group by `verdictalias1`.`verdictalias4`) as verdictalias5";
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testSelectAvgBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    String aliasName = "a";
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "mycolumn1")), aliasName)),
        base);
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewritten = rewriter.rewrite(relation);
    
    String aliasForSumEstimate = AliasRenamingRules.sumEstimateAliasName(aliasName);
    String aliasForSumScaledSubsum = AliasRenamingRules.sumScaledSumAliasName(aliasName);
    String aliasForSumSquaredScaledSubsum = AliasRenamingRules.sumSquaredScaledSumAliasName(aliasName);
    String aliasForCountEstimate = AliasRenamingRules.countEstimateAliasName(aliasName);
    String aliasForSumScaledSubcount = AliasRenamingRules.sumScaledCountAliasName(aliasName);
    String aliasForSumSquaredScaledSubcount = AliasRenamingRules.sumSquaredScaledCountAliasName(aliasName);
    String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
    String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

    for (int k = 0; k < aggblockCount; k++) {
      String expected = "select sum(`verdictalias5`.`verdictalias6`) as " + aliasForSumEstimate + ", "
          + "sum(`verdictalias5`.`verdictalias7`) as " + aliasForCountEstimate + ", "
          + "sum(`verdictalias5`.`verdictalias6` * "
          + "`verdictalias5`.`verdictalias8`) as " + aliasForSumScaledSubsum + ", "
          + "sum((`verdictalias5`.`verdictalias6` * `verdictalias5`.`verdictalias6`) * "
          + "`verdictalias5`.`verdictalias8`) as " + aliasForSumSquaredScaledSubsum + ", "
          + "sum(`verdictalias5`.`verdictalias7` * "
          + "`verdictalias5`.`verdictalias8`) as " + aliasForSumScaledSubcount + ", "
          + "sum((`verdictalias5`.`verdictalias7` * `verdictalias5`.`verdictalias7`) * "
          + "`verdictalias5`.`verdictalias8`) as " + aliasForSumSquaredScaledSubcount + ", "
          + "count(*) as " + aliasForCountSubsample + ", "
          + "sum(`verdictalias5`.`verdictalias8`) as " + aliasForSumSubsampleSize + " "
          + "from (select "
          + "sum(`verdictalias1`.`mycolumn1` / "
          + "(`verdictalias1`.`verdictalias2` + (`verdictalias1`.`verdictalias3` * " + k
          + "))) as verdictalias6, "    // subsum
          + "sum(case 1 when `verdictalias1`.`mycolumn1` is not null else 0 end"
          + " / (`verdictalias1`.`verdictalias2` + (`verdictalias1`.`verdictalias3` * " + k
          + "))) as verdictalias7, "    // subcount
          + "sum(case 1 when `verdictalias1`.`mycolumn1` is not null else 0 end) as verdictalias8 "   // subsample size
          + "from (select *, `t`.`verdictincprob` as verdictalias2, "
          + "`t`.`verdictincprobblockdiff` as verdictalias3, "
          + "`t`.`verdictsid` as verdictalias4 "
          + "from `myschema`.`mytable` as t "
          + "where `t`.`verdictpartition` = " + k + ") as verdictalias1 "
          + "group by `verdictalias1`.`verdictalias4`) as verdictalias5";
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testSelectSumGroupbyBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    String aliasName = "a";
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("t", "mygroup"), "mygroup"),
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("t", "mycolumn1")), aliasName)),
        base);
    relation.addGroupby(new BaseColumn("t", "mygroup"));
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewritten = rewriter.rewrite(relation);
    
    String aliasForSumEstimate = AliasRenamingRules.sumEstimateAliasName(aliasName);
    String aliasForSumScaledSubsum = AliasRenamingRules.sumScaledSumAliasName(aliasName);
    String aliasForSumSquaredScaledSubsum = AliasRenamingRules.sumSquaredScaledSumAliasName(aliasName);
    String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
    String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

    for (int k = 0; k < aggblockCount; k++) {
      String expected = "select `verdictalias5`.`verdictalias6` as mygroup, "
          + "sum(`verdictalias5`.`verdictalias7`) as " + aliasForSumEstimate + ", "
          + "sum(`verdictalias5`.`verdictalias7` * "
          + "`verdictalias5`.`verdictalias8`) as " + aliasForSumScaledSubsum + ", "
          + "sum((`verdictalias5`.`verdictalias7` * `verdictalias5`.`verdictalias7`) * "
          + "`verdictalias5`.`verdictalias8`) as " + aliasForSumSquaredScaledSubsum + ", "
          + "count(*) as " + aliasForCountSubsample + ", "
          + "sum(`verdictalias5`.`verdictalias8`) as " + aliasForSumSubsampleSize + " "
          + "from ("
          + "select `verdictalias1`.`mygroup` as verdictalias6, "
          + "sum(`verdictalias1`.`mycolumn1` / "
          + "(`verdictalias1`.`verdictalias2` + (`verdictalias1`.`verdictalias3` * " + k
          + "))) as verdictalias7, "
          + "sum(case 1 when `verdictalias1`.`mycolumn1` is not null else 0 end) as verdictalias8 "
          + "from (select *, `t`.`verdictincprob` as verdictalias2, "
          + "`t`.`verdictincprobblockdiff` as verdictalias3, "
          + "`t`.`verdictsid` as verdictalias4 "
          + "from `myschema`.`mytable` as t "
          + "where `t`.`verdictpartition` = " + k + ") as verdictalias1 "
          + "group by `verdictalias6`, `verdictalias1`.`verdictalias4`) as verdictalias5 "
          + "group by `mygroup`";
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testSelectSumGroupby2BaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    String aliasName = "a";
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("t", "mygroup"), "myalias"),
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("t", "mycolumn1")), aliasName)),
        base);
    relation.addGroupby(new AliasReference("myalias"));
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewritten = rewriter.rewrite(relation);
    
    String aliasForSumEstimate = AliasRenamingRules.sumEstimateAliasName(aliasName);
    String aliasForSumScaledSubsum = AliasRenamingRules.sumScaledSumAliasName(aliasName);
    String aliasForSumSquaredScaledSubsum = AliasRenamingRules.sumSquaredScaledSumAliasName(aliasName);
    String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
    String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

    for (int k = 0; k < aggblockCount; k++) {
      String expected = "select `verdictalias5`.`verdictalias6` as myalias, "
          + "sum(`verdictalias5`.`verdictalias7`) as " + aliasForSumEstimate + ", "
          + "sum(`verdictalias5`.`verdictalias7` * "
          + "`verdictalias5`.`verdictalias8`) as " + aliasForSumScaledSubsum + ", "
          + "sum((`verdictalias5`.`verdictalias7` * `verdictalias5`.`verdictalias7`) * "
          + "`verdictalias5`.`verdictalias8`) as " + aliasForSumSquaredScaledSubsum + ", "
          + "count(*) as " + aliasForCountSubsample + ", "
          + "sum(`verdictalias5`.`verdictalias8`) as " + aliasForSumSubsampleSize + " "
          + "from ("
          + "select `verdictalias1`.`mygroup` as verdictalias6, "
          + "sum(`verdictalias1`.`mycolumn1` / "
          + "(`verdictalias1`.`verdictalias2` + (`verdictalias1`.`verdictalias3` * " + k
          + "))) as verdictalias7, "
          + "sum(case 1 when `verdictalias1`.`mycolumn1` is not null else 0 end) as verdictalias8 "
          + "from (select *, `t`.`verdictincprob` as verdictalias2, "
          + "`t`.`verdictincprobblockdiff` as verdictalias3, "
          + "`t`.`verdictsid` as verdictalias4 "
          + "from `myschema`.`mytable` as t "
          + "where `t`.`verdictpartition` = " + k + ") as verdictalias1 "
          + "group by `verdictalias6`, `verdictalias1`.`verdictalias4`) as verdictalias5 "
          + "group by `myalias`";
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testSelectCountGroupbyBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    String aliasName = "a";
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("t", "mygroup"), "mygroup"),
            new AliasedColumn(new ColumnOp("count"), aliasName)), base);
    relation.addGroupby(new BaseColumn("t", "mygroup"));
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewritten = rewriter.rewrite(relation);
    
    String aliasForCountEstimate = AliasRenamingRules.countEstimateAliasName(aliasName);
    String aliasForSumScaledSubcount = AliasRenamingRules.sumScaledCountAliasName(aliasName);
    String aliasForSumSquaredScaledSubcount = AliasRenamingRules.sumSquaredScaledCountAliasName(aliasName);
    String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
    String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

    for (int k = 0; k < aggblockCount; k++) {
      String expected = "select `verdictalias5`.`verdictalias6` as mygroup, "
          + "sum(`verdictalias5`.`verdictalias7`) as " + aliasForCountEstimate + ", "
          + "sum(`verdictalias5`.`verdictalias7` * "
          + "`verdictalias5`.`verdictalias8`) as " + aliasForSumScaledSubcount + ", "
          + "sum((`verdictalias5`.`verdictalias7` * `verdictalias5`.`verdictalias7`) * "
          + "`verdictalias5`.`verdictalias8`) as " + aliasForSumSquaredScaledSubcount + ", "
          + "count(*) as " + aliasForCountSubsample + ", "
          + "sum(`verdictalias5`.`verdictalias8`) as " + aliasForSumSubsampleSize + " "
          + "from ("
          + "select `verdictalias1`.`mygroup` as verdictalias6, "
          + "sum(1 / "
          + "(`verdictalias1`.`verdictalias2` + (`verdictalias1`.`verdictalias3` * " + k
          + "))) as verdictalias7, "
          + "count(*) as verdictalias8 "
          + "from (select *, `t`.`verdictincprob` as verdictalias2, "
          + "`t`.`verdictincprobblockdiff` as verdictalias3, "
          + "`t`.`verdictsid` as verdictalias4 "
          + "from `myschema`.`mytable` as t "
          + "where `t`.`verdictpartition` = " + k + ") as verdictalias1 "
          + "group by `verdictalias6`, `verdictalias1`.`verdictalias4`) as verdictalias5 "
          + "group by `mygroup`";

      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testSelectAvgGroupbyBaseTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    String aliasName = "a";
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("t", "mygroup"), "mygroup"),
            new AliasedColumn(new ColumnOp("avg", new BaseColumn("t", "mycolumn1")), aliasName)),
        base);
    relation.addGroupby(new BaseColumn("t", "mygroup"));
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewritten = rewriter.rewrite(relation);
    
    String aliasForSumEstimate = AliasRenamingRules.sumEstimateAliasName(aliasName);
    String aliasForSumScaledSubsum = AliasRenamingRules.sumScaledSumAliasName(aliasName);
    String aliasForSumSquaredScaledSubsum = AliasRenamingRules.sumSquaredScaledSumAliasName(aliasName);
    String aliasForCountEstimate = AliasRenamingRules.countEstimateAliasName(aliasName);
    String aliasForSumScaledSubcount = AliasRenamingRules.sumScaledCountAliasName(aliasName);
    String aliasForSumSquaredScaledSubcount = AliasRenamingRules.sumSquaredScaledCountAliasName(aliasName);
    String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
    String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

    for (int k = 0; k < aggblockCount; k++) {
      String expected = "select `verdictalias5`.`verdictalias6` as mygroup, "
          + "sum(`verdictalias5`.`verdictalias7`) as " + aliasForSumEstimate + ", "
          + "sum(`verdictalias5`.`verdictalias8`) as " + aliasForCountEstimate + ", "
          + "sum(`verdictalias5`.`verdictalias7` * "
          + "`verdictalias5`.`verdictalias9`) as " + aliasForSumScaledSubsum + ", "
          + "sum((`verdictalias5`.`verdictalias7` * `verdictalias5`.`verdictalias7`) * "
          + "`verdictalias5`.`verdictalias9`) as " + aliasForSumSquaredScaledSubsum + ", "
          + "sum(`verdictalias5`.`verdictalias8` * "
          + "`verdictalias5`.`verdictalias9`) as " + aliasForSumScaledSubcount + ", "
          + "sum((`verdictalias5`.`verdictalias8` * `verdictalias5`.`verdictalias8`) * "
          + "`verdictalias5`.`verdictalias9`) as " + aliasForSumSquaredScaledSubcount + ", "
          + "count(*) as " + aliasForCountSubsample + ", "
          + "sum(`verdictalias5`.`verdictalias9`) as " + aliasForSumSubsampleSize + " "
          + "from (select `verdictalias1`.`mygroup` as verdictalias6, "
          + "sum(`verdictalias1`.`mycolumn1` / "
          + "(`verdictalias1`.`verdictalias2` + (`verdictalias1`.`verdictalias3` * " + k
          + "))) as verdictalias7, "    // subsum
          + "sum(case 1 when `verdictalias1`.`mycolumn1` is not null else 0 end"
          + " / (`verdictalias1`.`verdictalias2` + (`verdictalias1`.`verdictalias3` * " + k
          + "))) as verdictalias8, "    // subcount
          + "sum(case 1 when `verdictalias1`.`mycolumn1` is not null else 0 end) as verdictalias9 "   // subsample size
          + "from (select *, `t`.`verdictincprob` as verdictalias2, "
          + "`t`.`verdictincprobblockdiff` as verdictalias3, "
          + "`t`.`verdictsid` as verdictalias4 "
          + "from `myschema`.`mytable` as t "
          + "where `t`.`verdictpartition` = " + k + ") as verdictalias1 "
          + "group by `verdictalias6`, `verdictalias1`.`verdictalias4`) as verdictalias5 "
          + "group by `mygroup`";
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testSelectSumNestedTable() throws VerdictDbException {
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    String aliasName = "a";
    SelectQueryOp nestedSource = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new AliasedColumn(
                ColumnOp.multiply(new BaseColumn("t", "price"), new BaseColumn("t", "discount")),
                "discounted_price")),
        base);
    nestedSource.setAliasName("s");
    SelectQueryOp relation = SelectQueryOp.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new ColumnOp("sum", new BaseColumn("s", "discounted_price")), aliasName)),
        nestedSource);
    ScrambleMeta meta = generateTestScrambleMeta();
    AggQueryRewriter rewriter = new AggQueryRewriter(meta);
    List<AbstractRelation> rewritten = rewriter.rewrite(relation);
    
    String aliasForSumEstimate = AliasRenamingRules.sumEstimateAliasName(aliasName);
    String aliasForSumScaledSubsum = AliasRenamingRules.sumScaledSumAliasName(aliasName);
    String aliasForSumSquaredScaledSubsum = AliasRenamingRules.sumSquaredScaledSumAliasName(aliasName);
    String aliasForCountSubsample = AliasRenamingRules.countSubsampleAliasName();
    String aliasForSumSubsampleSize = AliasRenamingRules.sumSubsampleSizeAliasName();

    for (int k = 0; k < aggblockCount; k++) {
      String expected = "select sum(`verdictalias8`.`verdictalias9`) as " + aliasForSumEstimate + ", "
          + "sum(`verdictalias8`.`verdictalias9` * "
          + "`verdictalias8`.`verdictalias10`) as " + aliasForSumScaledSubsum + ", "
          + "sum((`verdictalias8`.`verdictalias9` * `verdictalias8`.`verdictalias9`) * "
          + "`verdictalias8`.`verdictalias10`) as " + aliasForSumSquaredScaledSubsum + ", "
          + "count(*) as " + aliasForCountSubsample + ", "
          + "sum(`verdictalias8`.`verdictalias10`) as " + aliasForSumSubsampleSize + " "
          + "from ("
          + "select sum(`s`.`discounted_price` / "
          + "(`s`.`verdictalias5` + (`s`.`verdictalias6` * " + k
          + "))) as verdictalias9, "
          + "sum(case 1 when `s`.`discounted_price` is not null else 0 end) as verdictalias10 "
          + "from (select `verdictalias1`.`price` * `verdictalias1`.`discount` as discounted_price, "
          + "`verdictalias1`.`verdictalias2` as verdictalias5, "
          + "`verdictalias1`.`verdictalias3` as verdictalias6, "
          + "`verdictalias1`.`verdictalias4` as verdictalias7 "
          + "from (select *, `t`.`verdictincprob` as verdictalias2, "
          + "`t`.`verdictincprobblockdiff` as verdictalias3, "
          + "`t`.`verdictsid` as verdictalias4 "
          + "from `myschema`.`mytable` as t "
          + "where `t`.`verdictpartition` = " + k + ") as verdictalias1) as s "
          + "group by `s`.`verdictalias7`) as verdictalias8";
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }
}
