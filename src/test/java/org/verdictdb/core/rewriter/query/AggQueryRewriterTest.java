package org.verdictdb.core.rewriter.query;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.verdictdb.core.query.AbstractRelation;
import org.verdictdb.core.query.AliasReference;
import org.verdictdb.core.query.AliasedColumn;
import org.verdictdb.core.query.BaseColumn;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.SelectItem;
import org.verdictdb.core.query.SelectQueryOp;
import org.verdictdb.core.rewriter.ScrambleMeta;
import org.verdictdb.core.sql.SelectQueryToSql;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.sql.syntax.HiveSyntax;

public class AggQueryRewriterTest {

  int aggblockCount = 10;

  ScrambleMeta generateTestScrambleMeta() {
    ScrambleMeta meta = new ScrambleMeta();
    meta.insertScrambleMetaEntry("myschema", "mytable",
        "verdictpartition", "verdictsid", "verdicttier",
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
      String expected = "select `verdictalias4`.`verdictalias5` as verdicttier, "
          + "sum(`verdictalias4`.`verdictalias6`) as " + aliasForSumEstimate + ", "
          + "sum(`verdictalias4`.`verdictalias6` * "
          + "`verdictalias4`.`verdictalias7`) as " + aliasForSumScaledSubsum + ", "
          + "sum((`verdictalias4`.`verdictalias6` * `verdictalias4`.`verdictalias6`) * "
          + "`verdictalias4`.`verdictalias7`) as " + aliasForSumSquaredScaledSubsum + ", "
          + "count(*) as " + aliasForCountSubsample + ", "
          + "sum(`verdictalias4`.`verdictalias7`) as " + aliasForSumSubsampleSize + " "
          + "from ("
          + "select `verdictalias1`.`verdictalias3` as verdictalias5, "
          + "sum(`verdictalias1`.`mycolumn1`) as verdictalias6, "
          + "sum(case when `verdictalias1`.`mycolumn1` is not null then 1 else 0 end) as verdictalias7 "
          + "from (select *, "
          + "`t`.`verdictsid` as verdictalias2, "
          + "`t`.`verdicttier` as verdictalias3 "
          + "from `myschema`.`mytable` as t "
          + "where `t`.`verdictpartition` = " + k + ") as verdictalias1 "
          + "group by `verdictalias1`.`verdictalias2`, `verdictalias5`) as verdictalias4 "
          + "group by `verdicttier`";
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
      String expected = "select `verdictalias4`.`verdictalias5` as verdicttier, "
          + "sum(`verdictalias4`.`verdictalias6`) as " + aliasForCountEstimate + ", "
          + "sum(`verdictalias4`.`verdictalias6` * "
          + "`verdictalias4`.`verdictalias6`) as " + aliasForSumScaledSubcount + ", "
          + "sum(pow(`verdictalias4`.`verdictalias6`, 3)) as " + aliasForSumSquaredScaledSubcount + ", "
          + "count(*) as " + aliasForCountSubsample + ", "
          + "sum(`verdictalias4`.`verdictalias6`) as " + aliasForSumSubsampleSize + " "
          + "from ("
          + "select `verdictalias1`.`verdictalias3` as verdictalias5, "
          + "count(*) as verdictalias6 "
          + "from (select *, "
          + "`t`.`verdictsid` as verdictalias2, "
          + "`t`.`verdicttier` as verdictalias3 "
          + "from `myschema`.`mytable` as t "
          + "where `t`.`verdictpartition` = " + k + ") as verdictalias1 "
          + "group by `verdictalias1`.`verdictalias2`, `verdictalias5`) as verdictalias4 "
          + "group by `verdicttier`";
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
      String expected = "select `verdictalias4`.`verdictalias5` as verdicttier, "
          + "sum(`verdictalias4`.`verdictalias6`) as " + aliasForSumEstimate + ", "
          + "sum(`verdictalias4`.`verdictalias7`) as " + aliasForCountEstimate + ", "
          + "sum(`verdictalias4`.`verdictalias6` * "
          + "`verdictalias4`.`verdictalias7`) as " + aliasForSumScaledSubsum + ", "
          + "sum((`verdictalias4`.`verdictalias6` * `verdictalias4`.`verdictalias6`) * "
          + "`verdictalias4`.`verdictalias7`) as " + aliasForSumSquaredScaledSubsum + ", "
          + "sum(`verdictalias4`.`verdictalias7` * "
          + "`verdictalias4`.`verdictalias7`) as " + aliasForSumScaledSubcount + ", "
          + "sum(pow(`verdictalias4`.`verdictalias7`, 3)) as " + aliasForSumSquaredScaledSubcount + ", "
          + "count(*) as " + aliasForCountSubsample + ", "
          + "sum(`verdictalias4`.`verdictalias7`) as " + aliasForSumSubsampleSize + " "
          + "from (select "
          + "`verdictalias1`.`verdictalias3` as verdictalias5, "
          + "sum(`verdictalias1`.`mycolumn1`) as verdictalias6, "    // subsum
          + "sum(case when `verdictalias1`.`mycolumn1` is not null then 1 else 0 end) as verdictalias7 "    // subsample size
          + "from (select *, "
          + "`t`.`verdictsid` as verdictalias2, "
          + "`t`.`verdicttier` as verdictalias3 "
          + "from `myschema`.`mytable` as t "
          + "where `t`.`verdictpartition` = " + k + ") as verdictalias1 "
          + "group by `verdictalias1`.`verdictalias2`, `verdictalias5`) as verdictalias4 "
          + "group by `verdicttier`";
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
      String expected = "select `verdictalias4`.`verdictalias5` as verdicttier, "
          + "`verdictalias4`.`verdictalias6` as mygroup, "
          + "sum(`verdictalias4`.`verdictalias7`) as " + aliasForSumEstimate + ", "
          + "sum(`verdictalias4`.`verdictalias7` * "
          + "`verdictalias4`.`verdictalias8`) as " + aliasForSumScaledSubsum + ", "
          + "sum((`verdictalias4`.`verdictalias7` * `verdictalias4`.`verdictalias7`) * "
          + "`verdictalias4`.`verdictalias8`) as " + aliasForSumSquaredScaledSubsum + ", "
          + "count(*) as " + aliasForCountSubsample + ", "
          + "sum(`verdictalias4`.`verdictalias8`) as " + aliasForSumSubsampleSize + " "
          + "from ("
          + "select `verdictalias1`.`verdictalias3` as verdictalias5, "
          + "`verdictalias1`.`mygroup` as verdictalias6, "
          + "sum(`verdictalias1`.`mycolumn1`) as verdictalias7, "
          + "sum(case when `verdictalias1`.`mycolumn1` is not null then 1 else 0 end) as verdictalias8 "
          + "from (select *, "
          + "`t`.`verdictsid` as verdictalias2, "
          + "`t`.`verdicttier` as verdictalias3 "
          + "from `myschema`.`mytable` as t "
          + "where `t`.`verdictpartition` = " + k + ") as verdictalias1 "
          + "group by `verdictalias1`.`verdictalias2`, `verdictalias5`, `verdictalias6`) as verdictalias4 "
          + "group by `verdicttier`, `mygroup`";
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
      String expected = "select `verdictalias4`.`verdictalias5` as verdicttier, "
          + "`verdictalias4`.`verdictalias6` as myalias, "
          + "sum(`verdictalias4`.`verdictalias7`) as " + aliasForSumEstimate + ", "
          + "sum(`verdictalias4`.`verdictalias7` * "
          + "`verdictalias4`.`verdictalias8`) as " + aliasForSumScaledSubsum + ", "
          + "sum((`verdictalias4`.`verdictalias7` * `verdictalias4`.`verdictalias7`) * "
          + "`verdictalias4`.`verdictalias8`) as " + aliasForSumSquaredScaledSubsum + ", "
          + "count(*) as " + aliasForCountSubsample + ", "
          + "sum(`verdictalias4`.`verdictalias8`) as " + aliasForSumSubsampleSize + " "
          + "from ("
          + "select `verdictalias1`.`verdictalias3` as verdictalias5, "
          + "`verdictalias1`.`mygroup` as verdictalias6, "
          + "sum(`verdictalias1`.`mycolumn1`) as verdictalias7, "
          + "sum(case when `verdictalias1`.`mycolumn1` is not null then 1 else 0 end) as verdictalias8 "
          + "from (select *, "
          + "`t`.`verdictsid` as verdictalias2, "
          + "`t`.`verdicttier` as verdictalias3 "
          + "from `myschema`.`mytable` as t "
          + "where `t`.`verdictpartition` = " + k + ") as verdictalias1 "
          + "group by `verdictalias1`.`verdictalias2`, `verdictalias5`, `verdictalias6`) as verdictalias4 "
          + "group by `verdicttier`, `myalias`";
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
      String expected = "select `verdictalias4`.`verdictalias5` as verdicttier, "
          + "`verdictalias4`.`verdictalias6` as mygroup, "
          + "sum(`verdictalias4`.`verdictalias7`) as " + aliasForCountEstimate + ", "
          + "sum(`verdictalias4`.`verdictalias7` * "
          + "`verdictalias4`.`verdictalias7`) as " + aliasForSumScaledSubcount + ", "
          + "sum(pow(`verdictalias4`.`verdictalias7`, 3)) as " + aliasForSumSquaredScaledSubcount + ", "
          + "count(*) as " + aliasForCountSubsample + ", "
          + "sum(`verdictalias4`.`verdictalias7`) as " + aliasForSumSubsampleSize + " "
          + "from ("
          + "select `verdictalias1`.`verdictalias3` as verdictalias5, "
          + "`verdictalias1`.`mygroup` as verdictalias6, "
          + "count(*) as verdictalias7 "
          + "from (select *, "
          + "`t`.`verdictsid` as verdictalias2, "
          + "`t`.`verdicttier` as verdictalias3 "
          + "from `myschema`.`mytable` as t "
          + "where `t`.`verdictpartition` = " + k + ") as verdictalias1 "
          + "group by `verdictalias1`.`verdictalias2`, `verdictalias5`, `verdictalias6`) as verdictalias4 "
          + "group by `verdicttier`, `mygroup`";
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
      String expected = "select `verdictalias4`.`verdictalias5` as verdicttier, "
          + "`verdictalias4`.`verdictalias6` as mygroup, "
          + "sum(`verdictalias4`.`verdictalias7`) as " + aliasForSumEstimate + ", "
          + "sum(`verdictalias4`.`verdictalias8`) as " + aliasForCountEstimate + ", "
          + "sum(`verdictalias4`.`verdictalias7` * "
          + "`verdictalias4`.`verdictalias8`) as " + aliasForSumScaledSubsum + ", "
          + "sum((`verdictalias4`.`verdictalias7` * `verdictalias4`.`verdictalias7`) * "
          + "`verdictalias4`.`verdictalias8`) as " + aliasForSumSquaredScaledSubsum + ", "
          + "sum(`verdictalias4`.`verdictalias8` * "
          + "`verdictalias4`.`verdictalias8`) as " + aliasForSumScaledSubcount + ", "
          + "sum(pow(`verdictalias4`.`verdictalias8`, 3)) as " + aliasForSumSquaredScaledSubcount + ", "
          + "count(*) as " + aliasForCountSubsample + ", "
          + "sum(`verdictalias4`.`verdictalias8`) as " + aliasForSumSubsampleSize + " "
          + "from (select "
          + "`verdictalias1`.`verdictalias3` as verdictalias5, "
          + "`verdictalias1`.`mygroup` as verdictalias6, "
          + "sum(`verdictalias1`.`mycolumn1`) as verdictalias7, "    // subsum
          + "sum(case when `verdictalias1`.`mycolumn1` is not null then 1 else 0 end) as verdictalias8 "    // subsample size
          + "from (select *, "
          + "`t`.`verdictsid` as verdictalias2, "
          + "`t`.`verdicttier` as verdictalias3 "
          + "from `myschema`.`mytable` as t "
          + "where `t`.`verdictpartition` = " + k + ") as verdictalias1 "
          + "group by `verdictalias1`.`verdictalias2`, `verdictalias5`, `verdictalias6`) as verdictalias4 "
          + "group by `verdicttier`, `mygroup`";
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
      String expected = "select `verdictalias6`.`verdictalias7` as verdicttier, "
          + "sum(`verdictalias6`.`verdictalias8`) as " + aliasForSumEstimate + ", "
          + "sum(`verdictalias6`.`verdictalias8` * "
          + "`verdictalias6`.`verdictalias9`) as " + aliasForSumScaledSubsum + ", "
          + "sum((`verdictalias6`.`verdictalias8` * `verdictalias6`.`verdictalias8`) * "
          + "`verdictalias6`.`verdictalias9`) as " + aliasForSumSquaredScaledSubsum + ", "
          + "count(*) as " + aliasForCountSubsample + ", "
          + "sum(`verdictalias6`.`verdictalias9`) as " + aliasForSumSubsampleSize + " "
          + "from ("
          + "select `s`.`verdictalias5` as verdictalias7, "
          + "sum(`s`.`discounted_price`) as verdictalias8, "
          + "sum(case when `s`.`discounted_price` is not null then 1 else 0 end) as verdictalias9 "
          + "from (select `verdictalias1`.`price` * `verdictalias1`.`discount` as discounted_price, "
          + "`verdictalias1`.`verdictalias2` as verdictalias4, "
          + "`verdictalias1`.`verdictalias3` as verdictalias5 "
          + "from (select *, "
          + "`t`.`verdictsid` as verdictalias2, "
          + "`t`.`verdicttier` as verdictalias3 "
          + "from `myschema`.`mytable` as t "
          + "where `t`.`verdictpartition` = " + k + ") as verdictalias1) as s "
          + "group by `s`.`verdictalias4`, `verdictalias7`) as verdictalias6 "
          + "group by `verdicttier`";
      SelectQueryToSql relToSql = new SelectQueryToSql(new HiveSyntax());
      String actual = relToSql.toSql(rewritten.get(k));
      assertEquals(expected, actual);
    }
  }
}
