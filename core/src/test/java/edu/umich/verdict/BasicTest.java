package edu.umich.verdict;

import org.junit.runners.MethodSorters;
import org.junit.AfterClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import edu.umich.verdict.exceptions.VerdictException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(edu.umich.verdict.MinimumTest.class)
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
    public void test900DeleteSample() throws VerdictException {
        String sql = "delete samples of tpch1g.lineitem";
        vc.executeJdbcQuery(sql);
    }
    
    @Test
    public void test999DeleteDatabase() throws VerdictException {
        String sql = "delete database tpch1g.lineitem_verdict";
        vc.executeJdbcQuery(sql);
    }
    
    @AfterClass
    public static void destroy() throws VerdictException {
        vc.destroy();
    }

}
