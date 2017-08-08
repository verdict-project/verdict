package edu.umich.verdict.tpch;

import java.io.FileNotFoundException;

import edu.umich.verdict.BaseIT;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class TpchQuery17 {

    public static void main(String[] args) throws FileNotFoundException, VerdictException {
        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost(BaseIT.readHost());
        conf.setPort("21050");
        conf.setDbmsSchema("tpch1g");
        conf.set("loglevel", "debug");

        VerdictContext vc = VerdictJDBCContext.from(conf);
        String sql = "select\n" + 
                " sum(l_extendedprice) / 7.0 as avg_yearly\n" + 
                "from\n" + 
                " lineitem inner join part on l_partkey = p_partkey\n" + 
                " inner join (\n" + 
                "   select l_partkey as partkey, 0.2 * avg(l_quantity) as small_quantity\n" + 
                "   from lineitem inner join part on l_partkey = p_partkey\n" +
                "   group by l_partkey) t\n" + 
                " on l_partkey = partkey\n" + 
                "where\n" + 
                " p_brand = 'Brand#23'\n" + 
                " and p_container = 'MED BOX'\n" + 
                " and L_quantity < small_quantity;\n";
        vc.executeJdbcQuery(sql);

        vc.destroy();
    }

}
