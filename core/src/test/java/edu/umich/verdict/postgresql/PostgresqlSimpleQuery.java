package edu.umich.verdict.postgresql;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;


import java.io.FileNotFoundException;
import java.sql.SQLException;

import static edu.umich.verdict.postgresql.PostgresqlBasicTest.connect;
import edu.umich.verdict.postgresql.PostgresqlBasicTest;

public class PostgresqlSimpleQuery {
    public static PostgresqlBasicTest postgresqlBasicTest;
    public static void main(String[] args) throws VerdictException, FileNotFoundException {

        VerdictConf conf = new VerdictConf();
        conf.setDbms("postgresql");
        conf.setHost("localhost");
        conf.setPort("5432");
        conf.setDbmsSchema("tpch1g");
        conf.setUser("postgres");
        conf.setPassword("zhongshucheng123");
        conf.set("loglevel", "debug");

        VerdictContext vc = VerdictJDBCContext.from(conf);
        String sql = "select count(*) from tpch1g.lineitem";
        vc.executeJdbcQuery(sql);
        vc.destroy();
    }
}
