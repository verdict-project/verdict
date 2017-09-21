/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import com.google.common.base.Joiner;

import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.ApproxRelation;
import edu.umich.verdict.relation.ApproxSingleRelation;

public class SparkAggregationIT extends AggregationIT {

    static SparkContext sc;

    static HiveContext hc;

    static VerdictSpark2HiveContext vc;

    static String database = "instacart1g";

    public static void setSparkContext(SparkContext sc) {
        SparkAggregationIT.sc = sc;
    }

    public static VerdictSpark2HiveContext getVerdictContext() {
        return vc;
    }

    private static void setup() {
        try {
            vc = new VerdictSpark2HiveContext(sc);
            vc.sql("use " + database);
            hc = new HiveContext(sc);
            hc.sql("use " + database);
        } catch (VerdictException e) {
            e.printStackTrace();
        }
    }

    public static void runAll(SparkContext sc) {
        setSparkContext(sc);
        setup();

        JUnitCore jcore = new JUnitCore();
        Result result = jcore.run(edu.umich.verdict.SparkAggregationIT.class);

        List<Failure> failures = result.getFailures();
        for (Failure f : failures) {
            System.out.println(f.getTrace());
        }

        System.out.println("Total number of test cases: " + result.getRunCount());
        System.out.println("Number of Failures: " + result.getFailureCount());
    }

    public static String[] test_methods = { "simpleAvg", "simpleAvg2", "simpleCount", "simpleSum", "simpleSum2" };

    public static void run(SparkContext sc) {
        setSparkContext(sc);
        setup();

        int totalTestCount = 0;
        int failureCount = 0;

        for (String name : test_methods) {
            totalTestCount++;
            Request request = Request.method(edu.umich.verdict.SparkAggregationIT.class, name);
            JUnitCore jcore = new JUnitCore();
            Result result = jcore.run(request);

            if (result.getFailureCount() > 0) {
                failureCount++;
                List<Failure> failures = result.getFailures();
                for (Failure f : failures) {
                    System.out.println(f.getTrace());
                }
            }
        }

        System.out.println("All tests finished");
        System.out.println("Total number of test cases: " + totalTestCount);
        System.out.println("Number of Successes: " + (totalTestCount - failureCount));
        System.out.println("Number of Failures: " + failureCount);
    }

    protected List<List<Object>> collectResult(DataFrame df) {
        List<List<Object>> result = new ArrayList<List<Object>>();
        List<Row> rows = df.collectAsList();
        for (Row row : rows) {
            int colCount = row.size();
            List<Object> arow = new ArrayList<Object>();
            for (int i = 0; i < colCount; i++) {
                arow.add(row.get(i));
            }
            result.add(arow);
        }
        return result;
    }

    @Override
    protected void testSimpleAggQuery(String sql) throws VerdictException {
        List<List<Object>> expected = collectResult(hc.sql(sql));
        List<List<Object>> actual = collectResult(vc.sql(sql));
        printTestCase(sql, expected, actual);
        assertColsSimilar(expected, actual, 1, error);
    }

    @Override
    protected void testGroupbyAggQuery(String sql) throws SQLException, VerdictException {
        List<List<Object>> expected = collectResult(hc.sql(sql));
        List<List<Object>> actual = collectResult(vc.sql(sql));
        printTestCase(sql, expected, actual);
        assertColsEqual(expected, actual, 1);
        assertColsSimilar(expected, actual, 2, error);
    }

    @Override
    protected void testSimpleCountFor(String tableName, String sampleType, List<String> sampleColumns)
            throws SQLException, VerdictException {
        String sql = String.format("select count(*) from %s", tableName);
        List<List<Object>> expected = collectResult(hc.sql(sql));

        TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
        ApproxRelation r = ApproxSingleRelation.from(vc,
                new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
        List<List<Object>> actual = collectResult(r.count().collectDataFrame());

        printTestCase(sql, expected, actual);
        assertColsSimilar(expected, actual, 1, error);
    }

    @Override
    protected void testGroupbyCountFor(String tableName, List<String> groupby, String sampleType,
            List<String> sampleColumns) throws SQLException, VerdictException {
        String groups = Joiner.on(", ").join(groupby);
        String sql = String.format("select %s, count(*) from %s group by %s order by %s", groups, tableName, groups,
                groups);
        List<List<Object>> expected = collectResult(hc.sql(sql));

        TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
        ApproxRelation r = ApproxSingleRelation.from(vc,
                new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
        List<List<Object>> actual = collectResult(r.groupby(groups).count().orderby(groups).collectDataFrame());

        printTestCase(sql, expected, actual);
        assertColsEqual(expected, actual, 1);
        assertColsSimilar(expected, actual, 2, error);
    }

    @Override
    protected void testSimpleAvgFor(String tableName, String aggCol, String sampleType, List<String> sampleColumns)
            throws SQLException, VerdictException {
        String sql = String.format("select avg(%s) from %s", aggCol, tableName);
        List<List<Object>> expected = collectResult(hc.sql(sql));

        TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
        ApproxRelation r = ApproxSingleRelation.from(vc,
                new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
        List<List<Object>> actual = collectResult(r.avg(aggCol).collectDataFrame());

        printTestCase(sql, expected, actual);
        assertColsSimilar(expected, actual, 1, error);
    }

    @Override
    protected void testGroupbyAvgFor(String tableName, String aggCol, List<String> groupby, String sampleType,
            List<String> sampleColumns) throws SQLException, VerdictException {
        String groups = Joiner.on(", ").join(groupby);
        String sql = String.format("select %s, avg(%s) from %s group by %s order by %s", groups, aggCol, tableName,
                groups, groups);
        List<List<Object>> expected = collectResult(hc.sql(sql));

        TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
        ApproxRelation r = ApproxSingleRelation.from(vc,
                new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
        List<List<Object>> actual = collectResult(r.groupby(groups).avg(aggCol).orderby(groups).collectDataFrame());

        printTestCase(sql, expected, actual);
        assertColsEqual(expected, actual, 1);
        assertColsSimilar(expected, actual, 2, error);
    }

    @Override
    protected void testSimpleSumFor(String tableName, String aggCol, String sampleType, List<String> sampleColumns)
            throws SQLException, VerdictException {
        String sql = String.format("select sum(%s) from %s", aggCol, tableName);
        List<List<Object>> expected = collectResult(hc.sql(sql));

        TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
        ApproxRelation r = ApproxSingleRelation.from(vc,
                new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
        List<List<Object>> actual = collectResult(r.sum(aggCol).collectDataFrame());

        printTestCase(sql, expected, actual);
        assertColsSimilar(expected, actual, 1, error);
    }

    @Override
    protected void testGroupbySumFor(String tableName, String aggCol, List<String> groupby, String sampleType,
            List<String> sampleColumns) throws SQLException, VerdictException {
        String groups = Joiner.on(", ").join(groupby);
        String sql = String.format("select %s, sum(%s) from %s group by %s order by %s", groups, aggCol, tableName,
                groups, groups);
        List<List<Object>> expected = collectResult(hc.sql(sql));

        TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
        ApproxRelation r = ApproxSingleRelation.from(vc,
                new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
        List<List<Object>> actual = collectResult(r.groupby(groups).sum(aggCol).orderby(groups).collectDataFrame());

        printTestCase(sql, expected, actual);
        assertColsEqual(expected, actual, 1);
        assertColsSimilar(expected, actual, 2, error);
    }

    @Override
    protected void testSimpleCountDistinctFor(String tableName, String aggCol, String sampleType,
            List<String> sampleColumns) throws SQLException, VerdictException {
        String sql = String.format("select count(distinct %s) from %s", aggCol, tableName);
        List<List<Object>> expected = collectResult(hc.sql(sql));

        TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
        ApproxRelation r = ApproxSingleRelation.from(vc,
                new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
        List<List<Object>> actual = collectResult(r.countDistinct(aggCol).collectDataFrame());

        printTestCase(sql, expected, actual);
        assertColsSimilar(expected, actual, 1, error);
    }

    @Override
    protected void testGroupbyCountDistinctFor(String tableName, String aggCol, List<String> groupby, String sampleType,
            List<String> sampleColumns) throws SQLException, VerdictException {
        String groups = Joiner.on(", ").join(groupby);
        String sql = String.format("select %s, count(distinct %s) from %s group by %s order by %s", groups, aggCol,
                tableName, groups, groups);
        List<List<Object>> expected = collectResult(hc.sql(sql));

        TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
        ApproxRelation r = ApproxSingleRelation.from(vc,
                new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
        List<List<Object>> actual = collectResult(
                r.groupby(groups).countDistinct(aggCol).orderby(groups).collectDataFrame());

        printTestCase(sql, expected, actual);
        assertColsEqual(expected, actual, 1);
        assertColsSimilar(expected, actual, 2, error);
    }

}
