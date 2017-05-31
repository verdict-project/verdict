package edu.umich.verdict.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DefaultJDBCConnection {

	public DefaultJDBCConnection() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Connection connection = null;
		Class.forName("com.cloudera.hive.jdbc4.HS2Driver");
		connection = DriverManager.getConnection("jdbc:hive2://ec2-52-54-86-158.compute-1.amazonaws.com:10000");
	}

}
