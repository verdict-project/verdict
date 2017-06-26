package edu.umich.verdict;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.TypeCasting;

@Category(IntegrationTest.class)
public class ImpalaIT {
	
	private VerdictContext vc;
	
	private Statement stmt;
	
	private double error = 0.05;
	
	@Before
	public void connect() throws VerdictException, SQLException {
		final String host = "salat1.eecs.umich.edu";
		final String port = "21050";
		final String schema = "instacart1g";
		
		VerdictConf conf = new VerdictConf();
		conf.setDbms("impala");
		conf.setHost(host);
		conf.setPort(port);
		conf.setDbmsSchema(schema);
		conf.set("no_user_password", "true");
		vc = new VerdictContext(conf);
		
		String url = String.format("jdbc:impala://%s:%s/%s", host, port, schema);
		Connection conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
	}
	
	@Test
	public void selectLimit() throws VerdictException, SQLException {
		String sql = "select * from orders limit 5";
		vc.executeQuery(sql);
	}

	@Test
	public void SimpleAvg() throws VerdictException, SQLException {
		String sql = "select avg(days_since_prior) from orders";
		List<List<Object>> expected = collectResult(stmt.executeQuery(sql));
		List<List<Object>> actual = collectResult(vc.executeQuery(sql));
		printTestCase(sql, expected, actual);
		
		assertColsSimilar(expected, actual, 1, error);
	}
	
	@Test
	public void SimpleAvg2() throws VerdictException, SQLException {
		String sql = "select avg(order_hour_of_day) from orders";
		List<List<Object>> expected = collectResult(stmt.executeQuery(sql));
		List<List<Object>> actual = collectResult(vc.executeQuery(sql));
		printTestCase(sql, expected, actual);
		
		assertColsSimilar(expected, actual, 1, error);
	}
	
	@Test
	public void SimpleCount() throws VerdictException, SQLException {
		String sql = "select count(*) from orders";
		List<List<Object>> expected = collectResult(stmt.executeQuery(sql));
		List<List<Object>> actual = collectResult(vc.executeQuery(sql));
		printTestCase(sql, expected, actual);
		
		assertColsSimilar(expected, actual, 1, error);
	}
	
	@Test
	public void SimpleSum() throws VerdictException, SQLException {
		String sql = "select sum(days_since_prior) from orders";
		List<List<Object>> expected = collectResult(stmt.executeQuery(sql));
		List<List<Object>> actual = collectResult(vc.executeQuery(sql));
		printTestCase(sql, expected, actual);
		
		assertColsSimilar(expected, actual, 1, error);
	}
	
	@Test
	public void SimpleSum2() throws VerdictException, SQLException {
		String sql = "select sum(order_hour_of_day) from orders";
		List<List<Object>> expected = collectResult(stmt.executeQuery(sql));
		List<List<Object>> actual = collectResult(vc.executeQuery(sql));
		printTestCase(sql, expected, actual);
		
		assertColsSimilar(expected, actual, 1, error);
	}
	
	@Test
	public void SimpleGroupbyAvg() throws VerdictException, SQLException {
		String sql = "select order_dow, avg(days_since_prior) from orders group by order_dow order by order_dow";
		List<List<Object>> expected = collectResult(stmt.executeQuery(sql));
		List<List<Object>> actual = collectResult(vc.executeQuery(sql));
		printTestCase(sql, expected, actual);
		
		assertColsEqual(expected, actual, 1);
		assertColsSimilar(expected, actual, 2, error);
	}
	
	@Test
	public void SimpleGroupbyCount() throws VerdictException, SQLException {
		String sql = "select order_dow, count(*) from orders group by order_dow order by order_dow";
		List<List<Object>> expected = collectResult(stmt.executeQuery(sql));
		List<List<Object>> actual = collectResult(vc.executeQuery(sql));
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
	
	protected void assertColsEqual(List<List<Object>> expected, List<List<Object>> actual, int colIndex) throws SQLException {
		List<Object> col1 = getColumn(expected, colIndex);
		List<Object> col2 = getColumn(actual, colIndex);
		assertArrayEquals(col1.toArray(), col2.toArray());
	}
	
	protected void assertColsSimilar(List<List<Object>> expected, List<List<Object>> actual, int colIndex, double error) throws SQLException {
		List<Object> col1 = getColumn(expected, colIndex);
		List<Object> col2 = getColumn(actual, colIndex);
//		System.out.println(col1);
//		System.out.println(col2);
		assertEquals(col1.size(), col2.size());
		
		for (int i = 0; i < col1.size(); i++) {
			double exp = TypeCasting.toDouble(col1.get(i));
			double act = TypeCasting.toDouble(col2.get(i));
//			System.out.println(exp + " " + act);
			assertEquals(exp, act, exp*error);
		}
	}
	
	protected List<Object> getColumn(List<List<Object>> ll, int colIndex) throws SQLException {
		List<Object> column = new ArrayList<Object>();
		//			int colCount = rs.getMetaData().getColumnCount();
		for (int i = 0; i < ll.size(); i ++) {
			column.add(ll.get(i).get(colIndex-1));
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

}
