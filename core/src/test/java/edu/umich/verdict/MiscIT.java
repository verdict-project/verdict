package edu.umich.verdict;

import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import edu.umich.verdict.exceptions.VerdictException;

public class MiscIT extends BaseIT {

	protected void runSql(String sql) throws VerdictException {
		vc.executeQuery(sql);
	}

	@Test
	public void showDatabases() throws VerdictException {
		String sql = "show databases";
		runSql(sql);
	}
	
	@Test
	public void showTables() throws VerdictException {
		String sql = "show tables";
		runSql(sql);
	}
	
	@Test
	public void describeTables() throws VerdictException {
		String sql = "describe orders";
		runSql(sql);
	}
	
	@Test
	public void getColumnsTest() throws VerdictException, SQLException {
		List<Pair<String, String>> tabCols = vc.getDbms().getAllTableAndColumns("instacart1g");
		for (Pair<String, String> tabCol : tabCols) {
			System.out.println(tabCol.getLeft() + " " + tabCol.getRight());
		}
	}
}
