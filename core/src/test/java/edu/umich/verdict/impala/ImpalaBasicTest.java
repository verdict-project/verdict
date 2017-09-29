package edu.umich.verdict.impala;

import java.io.FileNotFoundException;
import java.sql.SQLException;

import org.junit.BeforeClass;

import edu.umich.verdict.BasicTest;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class ImpalaBasicTest extends BasicTest {

    static protected String dbname = "impala";
    
    @BeforeClass
    public static void connect() throws VerdictException, SQLException, FileNotFoundException, ClassNotFoundException {
        VerdictConf conf = new VerdictConf();
        conf.setHost(readHost());
        conf.setDbms(dbname);
        conf.setLoglevel("debug");

        vc = VerdictJDBCContext.from(conf);
    }

}
