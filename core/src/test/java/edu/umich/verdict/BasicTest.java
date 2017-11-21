package edu.umich.verdict;

import org.junit.runners.MethodSorters;
import org.junit.AfterClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.TestBase;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(MinimumTest.class)
public abstract class BasicTest extends TestBase {

    @Test
    public void test010CreateSample() throws VerdictException {
        String sql = "create uniform sample of tpch1g.lineitem";
        vc.executeJdbcQuery(sql);
    }
    
    @Test
    public void test100CountAll() throws VerdictException {
        String sql = "select count(*) from tpch1g.lineitem";
        vc.executeJdbcQuery(sql);
    }
    
    @Test
    public void test110NestedCountAll() throws VerdictException {
        String sql = "select count(*) from (select * from tpch1g.lineitem) t";
        vc.executeJdbcQuery(sql);
    }
    
    @Test
    public void test900DeleteSample() throws VerdictException {
        String sql = "delete samples of tpch1g.lineitem";
        vc.executeJdbcQuery(sql);
    }
    
    @Test
    public void test999DeleteDatabase() throws VerdictException {
        String suffix = vc.getConf().get("verdict.meta_data.meta_database_suffix");
        String sql = String.format("drop table tpch1g%s.verdict_meta_name", suffix);
        vc.executeJdbcQuery(sql);
        sql = String.format("drop table tpch1g%s.verdict_meta_size", suffix);
        vc.executeJdbcQuery(sql);
        sql = String.format("drop database tpch1g%s", suffix);
        vc.executeJdbcQuery(sql);
    }
    
    @AfterClass
    public static void destroy() throws VerdictException {
        vc.destroy();
    }

}
