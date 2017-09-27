/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.base.Joiner;

import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.ApproxRelation;
import edu.umich.verdict.relation.ApproxSingleRelation;
import edu.umich.verdict.util.TypeCasting;

public class AggregationIT extends BaseIT {

    protected void testSimpleAggQuery(String sql) throws SQLException, VerdictException {
        List<List<Object>> expected = collectResult(stmt.executeQuery(sql));
        List<List<Object>> actual = collectResult(vc.executeJdbcQuery(sql));
        printTestCase(sql, expected, actual);
        assertColsSimilar(expected, actual, 1, error);
    }

    protected void testGroupbyAggQuery(String sql) throws SQLException, VerdictException {
        List<List<Object>> expected = collectResult(stmt.executeQuery(sql));
        List<List<Object>> actual = collectResult(vc.executeJdbcQuery(sql));
        printTestCase(sql, expected, actual);
        assertColsEqual(expected, actual, 1);
        assertColsSimilar(expected, actual, 2, error);
    }

    protected void testSimpleCountFor(String tableName, String sampleType, List<String> sampleColumns)
            throws SQLException, VerdictException {
        String sql = String.format("select count(*) from %s", tableName);
        List<List<Object>> expected = collectResult(stmt.executeQuery(sql));

        TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
        ApproxRelation r = ApproxSingleRelation.from(vc,
                new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
        List<List<Object>> actual = collectResult(r.count().collectResultSet());

        printTestCase(sql, expected, actual);
        assertColsSimilar(expected, actual, 1, error);
    }

    protected void testGroupbyCountFor(String tableName, List<String> groupby, String sampleType,
            List<String> sampleColumns) throws SQLException, VerdictException {
        String groups = Joiner.on(", ").join(groupby);
        String sql = String.format("select %s, count(*) from %s group by %s order by %s", groups, tableName, groups,
                groups);
        List<List<Object>> expected = collectResult(stmt.executeQuery(sql));

        TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
        ApproxRelation r = ApproxSingleRelation.from(vc,
                new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
        List<List<Object>> actual = collectResult(r.groupby(groups).count().orderby(groups).collectResultSet());

        printTestCase(sql, expected, actual);
        assertColsEqual(expected, actual, 1);
        assertColsSimilar(expected, actual, 2, error);
    }

    protected void testSimpleAvgFor(String tableName, String aggCol, String sampleType, List<String> sampleColumns)
            throws SQLException, VerdictException {
        String sql = String.format("select avg(%s) from %s", aggCol, tableName);
        List<List<Object>> expected = collectResult(stmt.executeQuery(sql));

        TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
        ApproxRelation r = ApproxSingleRelation.from(vc,
                new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
        List<List<Object>> actual = collectResult(r.avg(aggCol).collectResultSet());

        printTestCase(sql, expected, actual);
        assertColsSimilar(expected, actual, 1, error);
    }

    protected void testGroupbyAvgFor(String tableName, String aggCol, List<String> groupby, String sampleType,
            List<String> sampleColumns) throws SQLException, VerdictException {
        String groups = Joiner.on(", ").join(groupby);
        String sql = String.format("select %s, avg(%s) from %s group by %s order by %s", groups, aggCol, tableName,
                groups, groups);
        List<List<Object>> expected = collectResult(stmt.executeQuery(sql));

        TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
        ApproxRelation r = ApproxSingleRelation.from(vc,
                new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
        List<List<Object>> actual = collectResult(r.groupby(groups).avg(aggCol).orderby(groups).collectResultSet());

        printTestCase(sql, expected, actual);
        assertColsEqual(expected, actual, 1);
        assertColsSimilar(expected, actual, 2, error);
    }

    protected void testSimpleSumFor(String tableName, String aggCol, String sampleType, List<String> sampleColumns)
            throws SQLException, VerdictException {
        String sql = String.format("select sum(%s) from %s", aggCol, tableName);
        List<List<Object>> expected = collectResult(stmt.executeQuery(sql));

        TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
        ApproxRelation r = ApproxSingleRelation.from(vc,
                new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
        List<List<Object>> actual = collectResult(r.sum(aggCol).collectResultSet());

        printTestCase(sql, expected, actual);
        assertColsSimilar(expected, actual, 1, error);
    }

    protected void testGroupbySumFor(String tableName, String aggCol, List<String> groupby, String sampleType,
            List<String> sampleColumns) throws SQLException, VerdictException {
        String groups = Joiner.on(", ").join(groupby);
        String sql = String.format("select %s, sum(%s) from %s group by %s order by %s", groups, aggCol, tableName,
                groups, groups);
        List<List<Object>> expected = collectResult(stmt.executeQuery(sql));

        TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
        ApproxRelation r = ApproxSingleRelation.from(vc,
                new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
        List<List<Object>> actual = collectResult(r.groupby(groups).sum(aggCol).orderby(groups).collectResultSet());

        printTestCase(sql, expected, actual);
        assertColsEqual(expected, actual, 1);
        assertColsSimilar(expected, actual, 2, error);
    }

    protected void testSimpleCountDistinctFor(String tableName, String aggCol, String sampleType,
            List<String> sampleColumns) throws SQLException, VerdictException {
        String sql = String.format("select count(distinct %s) from %s", aggCol, tableName);
        List<List<Object>> expected = collectResult(stmt.executeQuery(sql));

        TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
        ApproxRelation r = ApproxSingleRelation.from(vc,
                new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
        List<List<Object>> actual = collectResult(r.countDistinct(aggCol).collectResultSet());

        printTestCase(sql, expected, actual);
        assertColsSimilar(expected, actual, 1, error);
    }

    protected void testGroupbyCountDistinctFor(String tableName, String aggCol, List<String> groupby, String sampleType,
            List<String> sampleColumns) throws SQLException, VerdictException {
        String groups = Joiner.on(", ").join(groupby);
        String sql = String.format("select %s, count(distinct %s) from %s group by %s order by %s", groups, aggCol,
                tableName, groups, groups);
        List<List<Object>> expected = collectResult(stmt.executeQuery(sql));

        TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
        ApproxRelation r = ApproxSingleRelation.from(vc,
                new SampleParam(vc, originalTable, sampleType, samplingRatio, sampleColumns));
        List<List<Object>> actual = collectResult(
                r.groupby(groups).countDistinct(aggCol).orderby(groups).collectResultSet());

        printTestCase(sql, expected, actual);
        assertColsEqual(expected, actual, 1);
        assertColsSimilar(expected, actual, 2, error);
    }

    protected List<List<Object>> collectResult(ResultSet rs) throws SQLException {
        List<List<Object>> result = new ArrayList<List<Object>>();
        int colCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            List<Object> row = new ArrayList<Object>();
            for (int i = 1; i <= colCount; i++) {
                row.add(rs.getObject(i));
            }
            result.add(row);
        }
        return result;

    }

    protected void assertColsEqual(List<List<Object>> expected, List<List<Object>> actual, int colIndex) {
        List<Object> col1 = getColumn(expected, colIndex);
        List<Object> col2 = getColumn(actual, colIndex);
        assertArrayEquals(col1.toArray(), col2.toArray());
    }

    protected void assertColsSimilar(List<List<Object>> expected, List<List<Object>> actual, int colIndex,
            double error) {
        List<Object> col1 = getColumn(expected, colIndex);
        List<Object> col2 = getColumn(actual, colIndex);
        assertEquals(col1.size(), col2.size());

        double err_sum = 0;
        for (int i = 0; i < col1.size(); i++) {
            if (col1.get(i) == null || col1.get(i).toString().equals("null"))
                continue;
            double exp = TypeCasting.toDouble(col1.get(i));
            double act = TypeCasting.toDouble(col2.get(i));
            err_sum += Math.abs(exp - act) / Math.abs(exp);
            // assertEquals(exp, act, exp*error);
        }
        double avg_err = err_sum / (double) col1.size();
        System.out.println("average error: " + avg_err);
        assertEquals(avg_err, 0, error);
    }

    protected List<Object> getColumn(List<List<Object>> ll, int colIndex) {
        List<Object> column = new ArrayList<Object>();
        // int colCount = rs.getMetaData().getColumnCount();
        for (int i = 0; i < ll.size(); i++) {
            column.add(ll.get(i).get(colIndex - 1));
        }
        return column;
    }

    protected void printTestCase(String sql, List<List<Object>> expected, List<List<Object>> actual) {
        System.out.println("Query tested:");
        System.out.println("    " + sql + "\n");
        System.out.println("Expected results:");
        printListOfList(expected);
        System.out.println("Actual results:");
        printListOfList(actual);
    }

    protected void printListOfList(List<List<Object>> ll) {
        for (int i = 0; i < ll.size(); i++) {
            for (int j = 0; j < ll.get(i).size(); j++) {
                System.out.print(ll.get(i).get(j) + "\t");
            }
            System.out.println();
        }
        System.out.println();
    }
    
    @Test
    @Category(edu.umich.verdict.MinimumTest.class)
    public void basicSelectCount() throws VerdictException {
        String sql = "select count(*) from tpch1g.lineitem";
        vc.executeJdbcQuery(sql);
    }

    @Test
    public void selectLimit() throws VerdictException, SQLException {
        String sql = "select * from orders limit 5";
        vc.executeJdbcQuery(sql);
    }

    @Test
    public void simpleOrderby() throws VerdictException, SQLException {
        String sql = "select order_id, user_id, eval_set, order_number, order_dow from orders order by order_id limit 5";
        List<List<Object>> expected = collectResult(stmt.executeQuery(sql));
        List<List<Object>> actual = collectResult(vc.executeJdbcQuery(sql));
        printTestCase(sql, expected, actual);

        assertColsEqual(expected, actual, 1);
        assertColsEqual(expected, actual, 2);
        assertColsEqual(expected, actual, 3);
        assertColsEqual(expected, actual, 4);
        assertColsEqual(expected, actual, 5);
    }

    @Test
    public void simpleCountUsingUniformSample() throws VerdictException, SQLException {
        testSimpleCountFor("orders", "uniform", Arrays.<String>asList());
    }

    @Test
    public void simpleSumUsingUniformSample() throws VerdictException, SQLException {
        testSimpleSumFor("orders", "days_since_prior", "uniform", Arrays.<String>asList());
    }

    @Test
    public void simpleAvgUsingUniformSample() throws VerdictException, SQLException {
        testSimpleAvgFor("orders", "days_since_prior", "uniform", Arrays.<String>asList());
    }

    @Test
    public void groupbyCountUsingUniformSample() throws VerdictException, SQLException {
        testGroupbyCountFor("orders", Arrays.asList("order_hour_of_day"), "uniform", Arrays.<String>asList());
    }

    @Test
    public void groupbyCountUsingUniformSample2() throws VerdictException, SQLException {
        testGroupbyCountFor("orders", Arrays.asList("order_number"), "uniform", Arrays.<String>asList());
    }

    @Test
    public void groupbySumUsingUniformSample() throws VerdictException, SQLException {
        testGroupbySumFor("orders", "days_since_prior", Arrays.asList("order_hour_of_day"), "uniform",
                Arrays.<String>asList());
    }

    @Test
    public void groupbySumUsingUniformSample2() throws VerdictException, SQLException {
        testGroupbySumFor("orders", "days_since_prior", Arrays.asList("order_dow"), "uniform", Arrays.<String>asList());
    }

    @Test
    public void groupbyAvgUsingUniformSample() throws VerdictException, SQLException {
        testGroupbyAvgFor("orders", "days_since_prior", Arrays.asList("order_hour_of_day"), "uniform",
                Arrays.<String>asList());
    }

    @Test
    public void groupbyAvgUsingUniformSample2() throws VerdictException, SQLException {
        testGroupbyAvgFor("orders", "days_since_prior", Arrays.asList("order_dow"), "uniform", Arrays.<String>asList());
    }

    @Test
    public void simpleCountUsingUniverseSample() throws VerdictException, SQLException {
        testSimpleCountFor("orders", "universe", Arrays.<String>asList("order_id"));
    }

    @Test
    public void simpleCountUsingUniverseSample2() throws VerdictException, SQLException {
        testSimpleCountFor("orders", "universe", Arrays.<String>asList("user_id"));
    }

    @Test
    public void simpleSumUsingUniverseSample() throws VerdictException, SQLException {
        testSimpleSumFor("orders", "days_since_prior", "universe", Arrays.<String>asList("order_id"));
    }

    @Test
    public void simpleSumUsingUniverseSample2() throws VerdictException, SQLException {
        testSimpleSumFor("orders", "days_since_prior", "universe", Arrays.<String>asList("user_id"));
    }

    @Test
    public void simpleAvgUsingUniverseSample() throws VerdictException, SQLException {
        testSimpleAvgFor("orders", "days_since_prior", "universe", Arrays.<String>asList("order_id"));
    }

    @Test
    public void simpleAvgUsingUniverseSample2() throws VerdictException, SQLException {
        testSimpleAvgFor("orders", "days_since_prior", "universe", Arrays.<String>asList("user_id"));
    }

    @Test
    public void simpleCountDistinctUsingUniverseSample() throws VerdictException, SQLException {
        testSimpleCountDistinctFor("orders", "user_id", "universe", Arrays.asList("user_id"));
    }

    @Test
    public void simpleCountDistinctUsingUniverseSample2() throws VerdictException, SQLException {
        testSimpleCountDistinctFor("orders", "order_id", "universe", Arrays.asList("order_id"));
    }

    @Test
    public void groupbyCountUsingUniverseSample() throws VerdictException, SQLException {
        testGroupbyCountFor("orders", Arrays.asList("order_hour_of_day"), "universe",
                Arrays.<String>asList("order_id"));
    }

    @Test
    public void groupbyCountUsingUniverseSample2() throws VerdictException, SQLException {
        testGroupbyCountFor("orders", Arrays.asList("order_hour_of_day"), "universe", Arrays.<String>asList("user_id"));
    }

    @Test
    public void groupbySumUsingUniverseSample() throws VerdictException, SQLException {
        testGroupbySumFor("orders", "days_since_prior", Arrays.asList("order_hour_of_day"), "universe",
                Arrays.<String>asList("user_id"));
    }

    @Test
    public void groupbySumUsingUniverseSample2() throws VerdictException, SQLException {
        testGroupbySumFor("orders", "days_since_prior", Arrays.asList("order_hour_of_day"), "universe",
                Arrays.<String>asList("order_id"));
    }

    @Test
    public void groupbyAvgUsingUniverseSample() throws VerdictException, SQLException {
        testGroupbyAvgFor("orders", "days_since_prior", Arrays.asList("order_hour_of_day"), "universe",
                Arrays.<String>asList("user_id"));
    }

    @Test
    public void groupbyAvgUsingUniverseSample2() throws VerdictException, SQLException {
        testGroupbyAvgFor("orders", "days_since_prior", Arrays.asList("order_hour_of_day"), "universe",
                Arrays.<String>asList("order_id"));
    }

    @Test
    public void groupbyCountDistinctUsingUniverseSample() throws VerdictException, SQLException {
        testGroupbyCountDistinctFor("orders", "user_id", Arrays.asList("order_hour_of_day"), "universe",
                Arrays.asList("user_id"));
    }

    @Test
    public void groupbyCountDistinctUsingUniverseSample2() throws VerdictException, SQLException {
        testGroupbyCountDistinctFor("orders", "order_id", Arrays.asList("order_hour_of_day"), "universe",
                Arrays.asList("order_id"));
    }

    @Test
    public void simpleCountUsingStratifiedSample() throws VerdictException, SQLException {
        testSimpleCountFor("orders", "stratified", Arrays.<String>asList("order_number"));
    }

    @Test
    public void simpleAvgUsingStratifiedSample() throws VerdictException, SQLException {
        testSimpleAvgFor("orders", "days_since_prior", "stratified", Arrays.<String>asList("order_number"));
    }

    @Test
    public void simpleAvgUsingStratifiedSample2() throws VerdictException, SQLException {
        testSimpleAvgFor("orders", "days_since_prior", "stratified", Arrays.<String>asList("days_since_prior"));
    }

    @Test
    public void simpleAvgUsingStratifiedSample3() throws VerdictException, SQLException {
        testSimpleAvgFor("orders", "days_since_prior", "stratified", Arrays.<String>asList("order_hour_of_day"));
    }

    @Test
    public void simpleCountDistinctUsingStratifiedSample() throws VerdictException, SQLException {
        testSimpleCountDistinctFor("orders", "order_number", "stratified", Arrays.<String>asList("order_number"));
    }

    @Test
    public void simpleCountDistinctUsingStratifiedSample2() throws VerdictException, SQLException {
        testSimpleCountDistinctFor("orders", "order_hour_of_day", "stratified",
                Arrays.<String>asList("order_hour_of_day"));
    }

    @Test
    public void groupbyCountUsingStratifiedSample() throws VerdictException, SQLException {
        testGroupbyCountFor("orders", Arrays.asList("order_dow"), "stratified", Arrays.<String>asList("order_number"));
    }

    @Test
    public void groupbyCountUsingStratifiedSample2() throws VerdictException, SQLException {
        testGroupbyCountFor("orders", Arrays.asList("order_dow"), "stratified", Arrays.<String>asList("order_dow"));
    }

    @Test
    public void groupbyCountUsingStratifiedSample3() throws VerdictException, SQLException {
        testGroupbyCountFor("orders", Arrays.asList("order_dow"), "stratified",
                Arrays.<String>asList("order_hour_of_day"));
    }

    @Test
    public void groupbySumUsingStratifiedSample() throws VerdictException, SQLException {
        testGroupbySumFor("orders", "days_since_prior", Arrays.asList("order_dow"), "stratified",
                Arrays.<String>asList("order_number"));
    }

    @Test
    public void groupbySumUsingStratifiedSample2() throws VerdictException, SQLException {
        testGroupbySumFor("orders", "days_since_prior", Arrays.asList("order_dow"), "stratified",
                Arrays.<String>asList("order_dow"));
    }

    @Test
    public void groupbySumUsingStratifiedSample3() throws VerdictException, SQLException {
        testGroupbySumFor("orders", "days_since_prior", Arrays.asList("order_dow"), "stratified",
                Arrays.<String>asList("order_hour_of_day"));
    }

    @Test
    public void groupbyAvgUsingStratifiedSample() throws VerdictException, SQLException {
        testGroupbyAvgFor("orders", "days_since_prior", Arrays.asList("order_dow"), "stratified",
                Arrays.<String>asList("order_number"));
    }

    @Test
    public void groupbyAvgUsingStratifiedSample2() throws VerdictException, SQLException {
        testGroupbyAvgFor("orders", "days_since_prior", Arrays.asList("order_dow"), "stratified",
                Arrays.<String>asList("order_dow"));
    }

    @Test
    public void groupbyAvgUsingStratifiedSample3() throws VerdictException, SQLException {
        testGroupbyAvgFor("orders", "days_since_prior", Arrays.asList("order_dow"), "stratified",
                Arrays.<String>asList("order_hour_of_day"));
    }

    @Test
    public void simpleAvg() throws VerdictException, SQLException {
        String sql = "select avg(days_since_prior) from orders";
        testSimpleAggQuery(sql);
    }

    @Test
    public void simpleAvg2() throws VerdictException, SQLException {
        String sql = "select avg(order_hour_of_day) from orders";
        testSimpleAggQuery(sql);
    }

    @Test
    public void simpleCount() throws VerdictException, SQLException {
        String sql = "select count(*) from orders";
        testSimpleAggQuery(sql);
    }

    @Test
    public void simpleSum() throws VerdictException, SQLException {
        String sql = "select sum(days_since_prior) from orders";
        testSimpleAggQuery(sql);
    }

    @Test
    public void simpleSum2() throws VerdictException, SQLException {
        String sql = "select sum(order_hour_of_day) from orders";
        testSimpleAggQuery(sql);
    }

    @Test
    public void groupbyCount() throws VerdictException, SQLException {
        String sql = "select order_dow, count(*) from orders group by order_dow order by order_dow";
        testGroupbyAggQuery(sql);
    }

    @Test
    public void groupbyCount2() throws VerdictException, SQLException {
        String sql = "select order_number, count(*) from orders group by order_number order by order_number";
        testGroupbyAggQuery(sql);
    }

    @Test
    public void groupbyCount3() throws VerdictException, SQLException {
        String sql = "select order_hour_of_day, count(*) from orders group by order_hour_of_day order by order_hour_of_day";
        testGroupbyAggQuery(sql);
    }

    @Test
    public void groupbySum() throws VerdictException, SQLException {
        String sql = "select order_dow, sum(days_since_prior) from orders group by order_dow order by order_dow";
        testGroupbyAggQuery(sql);
    }

    @Test
    public void groupbySum2() throws VerdictException, SQLException {
        String sql = "select order_number, sum(days_since_prior) from orders group by order_number order by order_number";
        testGroupbyAggQuery(sql);
    }

    @Test
    public void groupbySum3() throws VerdictException, SQLException {
        String sql = "select order_hour_of_day, sum(days_since_prior) from orders group by order_hour_of_day order by order_hour_of_day";
        testGroupbyAggQuery(sql);
    }

    @Test
    public void groupbyAvg() throws VerdictException, SQLException {
        String sql = "select order_dow, avg(days_since_prior) from orders group by order_dow order by order_dow";
        testGroupbyAggQuery(sql);
    }

    @Test
    public void groupbyAvg2() throws VerdictException, SQLException {
        String sql = "select order_number, avg(days_since_prior) from orders group by order_number order by order_number";
        testGroupbyAggQuery(sql);
    }

    @Test
    public void groupbyAvg3() throws VerdictException, SQLException {
        String sql = "select order_hour_of_day, avg(days_since_prior) from orders group by order_hour_of_day order by order_hour_of_day";
        testGroupbyAggQuery(sql);
    }

}