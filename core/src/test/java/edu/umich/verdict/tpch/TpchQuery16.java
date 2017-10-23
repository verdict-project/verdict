package edu.umich.verdict.tpch;

import java.io.FileNotFoundException;

import edu.umich.verdict.TestBase;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class TpchQuery16 {

    public static void main(String[] args) throws VerdictException, FileNotFoundException {
        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost(TestBase.readHost());
        conf.setPort("21050");
        conf.setDbmsSchema("tpch1g");
        conf.set("loglevel", "debug");

        VerdictContext vc = VerdictJDBCContext.from(conf);
        String sql16 = "select\n" + 
                " p_brand,\n" + 
                " p_type,\n" + 
                " p_size,\n" + 
                " count(distinct ps_suppkey) as supplier_cnt\n" + 
                "from\n" + 
                " partsupp,\n" + 
                " part\n" + 
                "where\n" + 
                " p_partkey = ps_partkey\n" + 
                " and p_brand <> 'Brand#45'\n" + 
                " and p_type not like 'MEDIUM POLISHED%'\n" + 
                " and p_size in (49, 14, 23, 45, 19, 3, 36, 9)\n" + 
                " and ps_suppkey not in (\n" + 
                "  select\n" + 
                "   s_suppkey\n" + 
                "  from\n" + 
                "   supplier\n" + 
                "  where\n" + 
                "   s_comment like '%Customer%Complaints%'\n" + 
                "  )\n" + 
                "group by\n" + 
                " p_brand,\n" + 
                " p_type,\n" + 
                " p_size\n" + 
                "order by\n" + 
                " supplier_cnt desc,\n" + 
                " p_brand,\n" + 
                " p_type,\n" + 
                " p_size\n" + 
                "limit 10;\n";

        vc.executeJdbcQuery(sql16);

        vc.destroy();
    }

}
