package edu.umich.verdict.hive;

import java.io.FileNotFoundException;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.BeforeClass;

import edu.umich.verdict.MiscIT;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class HiveMiscIT extends MiscIT {

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
	public void getColumnsTest() throws VerdictException, SQLException {
		super.getColumnsTest();
	}

}
