package edu.umich.verdict;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.BeforeClass;

import edu.umich.verdict.exceptions.VerdictException;

public class ImpalaMiscIT extends MiscIT {
	
	@BeforeClass
	public static void connect() throws VerdictException, SQLException, FileNotFoundException {
		final String host = readHost();
		final String port = "21050";
		final String schema = "instacart1g";

		VerdictConf conf = new VerdictConf();
		conf.setDbms("impala");
		conf.setHost(host);
		conf.setPort(port);
		conf.setDbmsSchema(schema);
		conf.set("no_user_password", "true");
		vc = new VerdictJDBCContext(conf);

		String url = String.format("jdbc:impala://%s:%s/%s", host, port, schema);
		Connection conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
	}
	
}
