package edu.umich.verdict;

import java.io.FileNotFoundException;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import edu.umich.verdict.exceptions.VerdictException;

@Category(IntegrationTest.class)
public class HiveSampleIT extends SampleIT {

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
		vc = new VerdictJDBCContext(conf);
		
		String url = String.format("jdbc:hive2://%s:%s/%s", host, port, schema);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
	}
	
	@Override
	public void createStratifiedSampleTest() throws VerdictException {
		super.createStratifiedSampleTest();
	}

	@AfterClass
	public static void destroy() throws VerdictException, SQLException {
		stmt.close();
		conn.close();
		vc.destroy();
	}

	@Override
	public void createUniverseSampleTest() throws VerdictException {
		super.createUniverseSampleTest();
	}

	@Override
	public void createRecommendedSampleTest() throws VerdictException {
		super.createRecommendedSampleTest();
	}

	@Override
	public void getColumnNamesTest() throws VerdictException {
		super.getColumnNamesTest();
	}

	@Override
	public void dropRecommendedSampleTest() throws VerdictException {
		super.dropRecommendedSampleTest();
	}

	@Override
	public void createUniformSampleTest() throws VerdictException {
		// TODO Auto-generated method stub
		super.createUniformSampleTest();
	}

}
