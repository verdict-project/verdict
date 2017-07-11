package edu.umich.verdict;

import java.sql.ResultSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class SampleIT extends BaseIT {
	
	@Test
	public void showSampleTest() throws VerdictException {
		ResultSet rs = vc.executeJdbcQuery("show samples");
		ResultSetConversion.printResultSet(rs);
	}

	@Test
	public void createRecommendedSampleTest() throws VerdictException {
		vc.executeJdbcQuery("CREATE SAMPLE OF orders");
	}
	
	@Test
	public void createUniformSampleTest() throws VerdictException {
		vc.executeJdbcQuery("create uniform sample of orders");
	}
	
	@Test
	public void createStratifiedSampleTest() throws VerdictException {
		vc.executeJdbcQuery("create stratified sample of orders ON order_dow");
	}
	
	@Test
	public void createUniverseSampleTest() throws VerdictException {
		vc.executeJdbcQuery("CREATE UNIVERSE SAMPLE OF orders ON user_id");
	}
	
	@Test
	public void getColumnNamesTest() throws VerdictException {
		TableUniqueName orders = TableUniqueName.uname(vc, "orders");
		Set<String> columns = vc.getMeta().getColumns(orders);
		System.out.println(columns);
	}
	
	@Test
	public void dropRecommendedSampleTest() throws VerdictException {
		vc.executeJdbcQuery("DROP SAMPLE OF orders");
	}

}
