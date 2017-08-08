package edu.umich.verdict.tpch;

import java.io.FileNotFoundException;

import edu.umich.verdict.BaseIT;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class TpchQuery9 {

    public static void main(String[] args) throws FileNotFoundException, VerdictException {
        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost(BaseIT.readHost());
        conf.setPort("21050");
        conf.setDbmsSchema("tpch1g");
        conf.set("loglevel", "debug");

        VerdictContext vc = VerdictJDBCContext.from(conf);
        String sql = "select nation, o_year, sum(amount) as sum_profit\n" + 
                "from (\n" + 
                "  select\n" + 
                "    n_name as nation,\n" + 
                "    substr(o_orderdate, 0, 4) as o_year,\n" + 
                "    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\n" + 
                "  from\n" + 
                "    lineitem\n" + 
                "    inner join orders on o_orderkey = l_orderkey\n" + 
                "    inner join partsupp on ps_suppkey = l_suppkey\n" + 
                "    inner join part on p_partkey = ps_partkey\n" + 
                "    inner join supplier on s_suppkey = ps_suppkey\n" + 
                "    inner join nation on s_nationkey = n_nationkey\n" + 
                "  where p_name like '%green%') as profit\n" + 
                "group by nation, o_year\n" + 
                "order by nation, o_year desc";
        vc.executeJdbcQuery(sql);

        vc.destroy();
    }

}
