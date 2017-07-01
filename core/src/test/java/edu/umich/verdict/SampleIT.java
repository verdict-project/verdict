package edu.umich.verdict;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;

public class SampleIT extends BaseIT {

	@Test
	public void createRecommendedSample() throws VerdictException {
		vc.executeQuery("CREATE SAMPLE OF ORDERS");
	}
	
	@Test
	public void createStratifiedSample() throws VerdictException {
		vc.executeQuery("CREATE STRATIFIED SAMPLE OF ORDERS ON ORDER_DOW");
	}
	
	@Test
	public void getColumnNamesTest() throws VerdictException {
		TableUniqueName orders = TableUniqueName.uname(vc, "orders");
		List<String> columns = vc.getMeta().getColumnNames(orders);
		System.out.println(columns);
	}

}
