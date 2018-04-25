package edu.umich.verdict.postgresql;


import java.io.FileNotFoundException;
import java.sql.SQLException;

import edu.umich.verdict.VerdictContext;
import org.junit.BeforeClass;

import edu.umich.verdict.BasicTest;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class PostgresqlBasicTest extends BasicTest{
    static protected String dbname = "postgresql";

    @BeforeClass
    public static void connect() throws VerdictException, SQLException, FileNotFoundException, ClassNotFoundException {
        VerdictConf conf = new VerdictConf();
        conf.setDbms("postgresql");
        conf.setHost("localhost");
        conf.setPort("5432");
        conf.setDbmsSchema("tpch1g");
        conf.setUser("postgres");
        conf.setPassword("zhongshucheng123");
        conf.set("loglevel", "debug");

        vc = VerdictJDBCContext.from(conf);
    }

}
