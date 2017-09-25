package edu.umich.verdict.jdbc.impala;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.jdbc.TestBase;

public class ImpalaThroughVerdictJdbcTest extends TestBase {
    
    public static Connection conn;

    @BeforeClass
    public static void connect() throws VerdictException, SQLException, FileNotFoundException, ClassNotFoundException {
        Class.forName("edu.umich.verdict.jdbc.Driver");
        String host = readHost();
        String url = String.format("jdbc:verdict:impala://%s", host);
        conn = DriverManager.getConnection(url);
        
    }
    
    @Test
    @Category(edu.umich.verdict.jdbc.MinimumTest.class)
    public void basicSelectCount() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("select count(*) from tpch1g.lineitem");
        stmt.close();
    }

    @AfterClass
    public static void destroy() throws SQLException {
        conn.close();
    }

}
