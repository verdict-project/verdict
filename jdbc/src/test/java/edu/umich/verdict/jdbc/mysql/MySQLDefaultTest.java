package edu.umich.verdict.jdbc.mysql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

import edu.umich.verdict.exceptions.VerdictException;


public class MySQLDefaultTest {

    public MySQLDefaultTest() {
        // TODO Auto-generated constructor stub
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, VerdictException {

        Class.forName("com.mysql.jdbc.Driver");

        String url = "jdbc:mysql://localhost:3306/verdict?user=verdict&password=verdict";
        Connection conn = DriverManager.getConnection(url);
        DatabaseMetaData databaseMetaData = conn.getMetaData();

        ResultSet rs = databaseMetaData.getColumns("verdict", null, null, null);
        //		ResultSet rs = databaseMetaData.getTables("verdict", null, null, null);
        //		ResultSetConversion.printResultSet(rs);

        System.out.println("hello");
        SQLWarning warning = conn.getWarnings().getNextWarning();
        while (warning != null) {
            System.out.println(String.format("State: %s, Error: %s, Msg: %s",
                    warning.getSQLState(),
                    warning.getErrorCode(),
                    warning.getMessage()));
            warning = warning.getNextWarning();
        }

    }

}
