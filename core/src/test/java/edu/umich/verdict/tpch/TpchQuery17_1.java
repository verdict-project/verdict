package edu.umich.verdict.tpch;

import java.io.FileNotFoundException;

import edu.umich.verdict.BaseIT;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class TpchQuery17_1 {

    public static void main(String[] args) throws FileNotFoundException, VerdictException {
        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost(BaseIT.readHost());
        conf.setPort("21050");
        conf.setDbmsSchema("tpch1g");
        conf.set("loglevel", "debug");

        VerdictContext vc = VerdictJDBCContext.from(conf);
        String sql = "select *\n" + 
                "from lineitem\n" + 
                "     inner join part\n" + 
                "         on l_partkey = p_partkey\n" + 
                "     inner join (\n" + 
                "             select l_partkey as partkey, 0.2 * avg(l_quantity) as small_quantity\n" + 
                "             from lineitem inner join part on l_partkey = p_partkey\n" + 
                "             group by l_partkey) t\n" + 
                "         on l_partkey = partkey\n" + 
                "where p_brand = 'Brand#23' and\n" + 
                "      p_container = 'MED BOX' and\n" + 
                "      l_quantity < small_quantity\n" + 
                "limit 5;";
        vc.executeJdbcQuery(sql);

        vc.destroy();
    }
}
