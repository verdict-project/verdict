package edu.umich.verdict.tpch;

import java.io.FileNotFoundException;

import edu.umich.verdict.BaseIT;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class TpchQuery15 {

    public static void main(String[] args) throws FileNotFoundException, VerdictException {
        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost(BaseIT.readHost());
        conf.setPort("21050");
        conf.setDbmsSchema("tpch1g");
        conf.set("loglevel", "debug");

        VerdictContext vc = VerdictJDBCContext.from(conf);
        String sql = "select s_suppkey, s_name, s_address, s_phone, total_revenue\n" + 
                "from (\n" + 
                "    select l_suppkey as supplier_no,\n" + 
                "           sum(l_extendedprice * (1 - l_discount)) as total_revenue\n" + 
                "    from lineitem\n" + 
                "    where l_shipdate >= '1996-01-01' and l_shipdate < '1996-04-01'\n" + 
                "    group by l_suppkey) revenue_temp\n" + 
                "    inner join supplier\n" + 
                "    on s_suppkey = supplier_no\n" + 
                "where total_revenue = (\n" + 
                "        select\n" + 
                "            max(total_revenue)\n" + 
                "        from (\n" + 
                "            select l_suppkey as supplier_no,\n" + 
                "                   sum(l_extendedprice * (1 - l_discount)) as total_revenue\n" + 
                "            from lineitem\n" + 
                "            where l_shipdate >= '1996-01-01' and l_shipdate < '1996-04-01'\n" + 
                "            group by l_suppkey) revenue_temp)\n" + 
                "order by s_suppkey;\n";
        vc.executeJdbcQuery(sql);

        vc.destroy();
    }

}
