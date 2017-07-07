package edu.umich.verdict;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import edu.umich.verdict.exceptions.VerdictException;

@Category(IntegrationTest.class)
public class HiveAggregationIT extends AggregationIT {
	
	@BeforeClass
	public static void connect() throws VerdictException, SQLException, FileNotFoundException {
		final String host = readHost();
		final String port = "10000";
		final String schema = "instacart1g";
		
		VerdictConf conf = new VerdictConf();
		conf.setDbms("hive2");
		conf.setHost(host);
		conf.setPort(port);
		conf.setDbmsSchema(schema);
		conf.set("no_user_password", "true");
		vc = new VerdictContext(conf);
		
		String url = String.format("jdbc:hive2://%s:%s/%s", host, port, schema);
		Connection conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
	}
	
	@AfterClass
	public static void destroy() throws VerdictException {
		vc.destroy();
	}

	@Override
	public void simpleAvgUsingUniverseSample() throws VerdictException, SQLException {
		// TODO Auto-generated method stub
		super.simpleAvgUsingUniverseSample();
	}

	@Override
	public void simpleCountUsingUniverseSample() throws VerdictException, SQLException {
		// TODO Auto-generated method stub
		super.simpleCountUsingUniverseSample();
	}

	@Override
	public void groupbyCountDistinctUsingUniverseSample() throws VerdictException, SQLException {
		// TODO Auto-generated method stub
		super.groupbyCountDistinctUsingUniverseSample();
	}

	@Override
	public void groupbyCountDistinctUsingUniverseSample2() throws VerdictException, SQLException {
		// TODO Auto-generated method stub
		super.groupbyCountDistinctUsingUniverseSample2();
	}

	
}
