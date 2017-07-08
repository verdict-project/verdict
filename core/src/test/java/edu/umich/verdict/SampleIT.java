package edu.umich.verdict;

import java.sql.ResultSet;
import java.util.List;

import org.junit.Test;

import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class SampleIT extends BaseIT {
	
	@Test
	public void showSampleTest() throws VerdictException {
		ResultSet rs = vc.executeQuery("show samples");
		ResultSetConversion.printResultSet(rs);
	}

	@Test
	public void createRecommendedSampleTest() throws VerdictException {
		vc.executeQuery("CREATE SAMPLE OF orders");
	}
	
	@Test
	public void createUniformSampleTest() throws VerdictException {
		vc.executeQuery("create uniform sample of orders");
	}
	
	@Test
	public void createStratifiedSampleTest() throws VerdictException {
		vc.executeQuery("create stratified sample of orders ON order_dow");
	}
	
	@Test
	public void createUniverseSampleTest() throws VerdictException {
		vc.executeQuery("CREATE UNIVERSE SAMPLE OF orders ON user_id");
	}
	
	@Test
	public void getColumnNamesTest() throws VerdictException {
		TableUniqueName orders = TableUniqueName.uname(vc, "orders");
		List<String> columns = vc.getMeta().getColumnNames(orders);
		System.out.println(columns);
	}
	
	@Test
	public void dropRecommendedSampleTest() throws VerdictException {
		vc.executeQuery("DROP SAMPLE OF orders");
	}

}
