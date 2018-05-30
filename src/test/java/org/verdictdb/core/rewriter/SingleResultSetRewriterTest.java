package org.verdictdb.core.rewriter;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.verdictdb.core.resultset.ResultSetGroup;
import org.verdictdb.core.resultset.ResultSetMeasures;
import org.verdictdb.core.resultset.SingleResultSet;
import org.verdictdb.exception.ValueException;
import org.verdictdb.exception.VerdictDbException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.verdictdb.core.rewriter.AliasRenamingRules.*;

public class SingleResultSetRewriterTest {

  @Test
  public void testSanityCheck() throws VerdictDbException {
    String mysumAlias = "mysum";
    List<String> columnNames = Arrays.asList("group1", "group2",
        sumEstimateAliasName(mysumAlias),
        sumScaledSumAliasName(mysumAlias),
        sumSquaredScaledSumAliasName(mysumAlias),
        countSubsampleAliasName(),
        sumSubsampleSizeAliasName());
    SingleResultSet resultSet = new SingleResultSet(columnNames);

    SingleResultSetRewriter rewriter = new SingleResultSetRewriter(resultSet);
    List<String> nonaggColumns = Arrays.asList("group1", "group2");
    List<Pair<String, String>> aggColumns = Arrays.asList(Pair.of(mysumAlias, "sum"));
    SingleResultSet converted = rewriter.rewrite(nonaggColumns, aggColumns);
  }

  @Test(expected = ValueException.class)
  public void testSanityCheckFail() throws VerdictDbException {
    String mysumAlias = "mysum";
    List<String> columnNames = Arrays.asList("group1", "group2",
        sumEstimateAliasName(mysumAlias),
        sumScaledSumAliasName(mysumAlias),
        sumSquaredScaledSumAliasName(mysumAlias),
        countSubsampleAliasName(),
        sumSubsampleSizeAliasName());
    SingleResultSet resultSet = new SingleResultSet(columnNames);

    SingleResultSetRewriter rewriter = new SingleResultSetRewriter(resultSet);
    List<String> nonaggColumns = Arrays.asList("group1", "group2");
    List<Pair<String, String>> aggColumns = Arrays.asList(Pair.of(mysumAlias, "count"));
    SingleResultSet converted2 = rewriter.rewrite(nonaggColumns, aggColumns);
  }

  @Test
  public void testSumRewriting() {
    String mysumAlias = "mysum";
    List<String> attrNames = Arrays.asList(
        sumEstimateAliasName(mysumAlias),
        sumScaledSumAliasName(mysumAlias),
        sumSquaredScaledSumAliasName(mysumAlias),
        countSubsampleAliasName(),
        sumSubsampleSizeAliasName());
    List<Object> attrValues = Arrays.<Object>asList(1.0, 2.0, 3.0, 4, 5);
    ResultSetMeasures measures = new ResultSetMeasures(attrNames, attrValues);
    SingleResultSetRewriter rewriter = new SingleResultSetRewriter();
    ResultSetMeasures rewrittenMeasures =
        rewriter.rewriteMeasures(measures, Arrays.asList(Pair.of(mysumAlias, "sum")));

    Object sumExpectedValue = rewrittenMeasures.getAttributeValue(expectedValueAliasName(mysumAlias));
    Object sumExpectedError = rewrittenMeasures.getAttributeValue(expectedErrorAliasName(mysumAlias));
    assertEquals(sumExpectedValue, 1.0);
    assertEquals(sumExpectedError, Math.sqrt((3.0 - 2*2.0 + 1.0*5) / ((float) 4*5)));
  }

  @Test
  public void testCountRewriting() {
    String mycountAlias = "mycount";
    List<String> attrNames = Arrays.asList(
        countEstimateAliasName(mycountAlias),
        sumScaledCountAliasName(mycountAlias),
        sumSquaredScaledCountAliasName(mycountAlias),
        countSubsampleAliasName(),
        sumSubsampleSizeAliasName());
    List<Object> attrValues = Arrays.<Object>asList(1.0, 2.0, 3.0, 4, 5);
    ResultSetMeasures measures = new ResultSetMeasures(attrNames, attrValues);
    SingleResultSetRewriter rewriter = new SingleResultSetRewriter();
    ResultSetMeasures rewrittenMeasures =
        rewriter.rewriteMeasures(measures, Arrays.asList(Pair.of(mycountAlias, "count")));

    Object countExpectedValue = rewrittenMeasures.getAttributeValue(expectedValueAliasName(mycountAlias));
    Object countExpectedError = rewrittenMeasures.getAttributeValue(expectedErrorAliasName(mycountAlias));
    assertEquals(countExpectedValue, 1.0);
    assertEquals(countExpectedError, Math.sqrt((3.0 - 2*2.0 + 1.0*5) / ((float) 4*5)));
  }

  @Test
  public void testAvgRewriting() {
    String myavgAlias = "myavg";
    List<String> attrNames = Arrays.asList(
        sumEstimateAliasName(myavgAlias),
        sumScaledSumAliasName(myavgAlias),
        sumSquaredScaledSumAliasName(myavgAlias),
        countEstimateAliasName(myavgAlias),
        sumScaledCountAliasName(myavgAlias),
        sumSquaredScaledCountAliasName(myavgAlias),
        countSubsampleAliasName(),
        sumSubsampleSizeAliasName());
    List<Object> attrValues = Arrays.<Object>asList(7.0, 8.0, 9.0, 1.0, 2.0, 3.0, 4, 5);
    ResultSetMeasures measures = new ResultSetMeasures(attrNames, attrValues);
    SingleResultSetRewriter rewriter = new SingleResultSetRewriter();
    ResultSetMeasures rewrittenMeasures =
        rewriter.rewriteMeasures(measures, Arrays.asList(Pair.of(myavgAlias, "avg")));

    Object avgExpectedValue = rewrittenMeasures.getAttributeValue(expectedValueAliasName(myavgAlias));
    Object avgExpectedError = rewrittenMeasures.getAttributeValue(expectedErrorAliasName(myavgAlias));
    assertEquals(avgExpectedValue, 7.0);
    double sum_var = (9.0 - 2*8.0 + 7.0*7.0*5) / ((float) 4*5);
    double count_var = (3.0 - 2*2.0 + 1.0*5) / ((float) 4*5);
    double eps = 1e-6;
    assertEquals(
        (double) avgExpectedError,
        Math.sqrt(Math.pow(7.0/1.0, 2) * (sum_var/Math.pow(7.0,2)+count_var/Math.pow(1.0,2))),
        eps);
  }

  @Test
  public void testResultSetRewriting() throws VerdictDbException {
    String mysumAlias = "mysum";
    List<String> columnNames = Arrays.asList(
        sumEstimateAliasName(mysumAlias),
        sumScaledSumAliasName(mysumAlias),
        sumSquaredScaledSumAliasName(mysumAlias),
        countSubsampleAliasName(),
        sumSubsampleSizeAliasName());
    SingleResultSet resultSet = new SingleResultSet(columnNames);

    List<Object> attrValues = Arrays.<Object>asList(1.0, 2.0, 3.0, 4, 5);
    ResultSetMeasures measures = new ResultSetMeasures(columnNames, attrValues);
    resultSet.addRow(ResultSetGroup.empty(), measures);

    // rewriting
    SingleResultSetRewriter rewriter = new SingleResultSetRewriter(resultSet);
    List<String> nonaggColumns = Arrays.asList();
    List<Pair<String, String>> aggColumns = Arrays.asList(Pair.of(mysumAlias, "sum"));
    SingleResultSet converted = rewriter.rewrite(nonaggColumns, aggColumns);

    // assertions
    List<String> rewrittenColNames = converted.getColumnNames();
    assertTrue(rewrittenColNames.contains(expectedValueAliasName(mysumAlias)));
    assertTrue(rewrittenColNames.contains(expectedErrorAliasName(mysumAlias)));
  }

}
