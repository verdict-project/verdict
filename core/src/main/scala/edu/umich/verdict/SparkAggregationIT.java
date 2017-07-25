package edu.umich.verdict;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import edu.umich.verdict.exceptions.VerdictException;

class SparkAggregationIT extends FunSuite with SharedJavaSparkContext {
	
	static SQLContext sqlContext;
	
	static VerdictContext vc;
	
	static double err_bound = 0.05;

	public static void main(String[] args) throws VerdictException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		setup();
		
		int total_test_count = 0;
		int success_count = 0;
		
		SparkAggregationIT test = new SparkAggregationIT();
		Method[] methods = test.getClass().getMethods();
		for (Method m : methods) {
			if (m.getName().endsWith("Test")) {
				total_test_count++;
				boolean value = (Boolean) m.invoke(test);
				if (value) success_count++;
			}
		}
		
		System.out.println("Total number of test cases: " + total_test_count);
		System.out.println("Successes: " + success_count);
		System.out.println("Failures: " + (total_test_count - success_count));
	}
	
	static void setup() throws VerdictException {
		SparkContext sc = new SparkContext(new SparkConf().setAppName("Spark Aggregation Integration Tests"));
		sqlContext = new HiveContext(sc);
		vc = new VerdictSparkContext(sqlContext);
	}
	
	boolean assertArraysEqual(List<Row> expected, List<Row> actual, int col) {
		int size1 = expected.size();
		int size2 = actual.size();
		if (size1 != size2) return false;
		
		for (int i = 0; i < size1; i++) {
			if (expected.get(i).get(col).equals(actual.get(i).get(col))) {
				continue;
			} else {
				return false;
			}
		}
		return true;
	}
	
	boolean assertArraysSimilar(List<Row> expected, List<Row> actual, int col, double avg_err_bound) {
		int size1 = expected.size();
		int size2 = actual.size();
		if (size1 != size2) return false;
		
		double sum_err = 0;
		
		for (int i = 0; i < size1; i++) {
			double val1 = expected.get(i).getDouble(col);
			double val2 = actual.get(i).getDouble(col);
			sum_err += Math.abs(val1 - val2) / Math.abs(val1);
		}
		
		double avg_err = sum_err / size1;
		
		if (avg_err <= avg_err_bound) {
			return true;
		} else {
			return false;
		}
	}
	
	boolean testSimilarFor(String sql, List<Integer> sameCols, List<Integer> similarCols) {
		boolean good = true;
		try {
			List<Row> expected = sqlContext.sql(sql).collectAsList();
			List<Row> actual = vc.sql(sql).collectAsList();
			
			for (Integer col : sameCols) {
				if (assertArraysEqual(expected, actual, col)) {
					continue;
				} else {
					good = false;
					break;
				}	
			}
			
			if (good) {
				for (Integer col : similarCols) {
					if (assertArraysSimilar(expected, actual, col, err_bound)) {
						continue;
					} else {
						good = false;
						break;
					}
				}
			}
			
		} catch (VerdictException e) {
			good = false;
		}
		
		return good;
	}
	
	boolean simpleCountTest() {
		String sql = "select count(*) from instacart1g.orders";
		boolean success = testSimilarFor(sql, Arrays.<Integer>asList(), Arrays.asList(0));
		return success;
	}
	
	boolean groupbyCountTest() {
		return true;
	}
	
}

