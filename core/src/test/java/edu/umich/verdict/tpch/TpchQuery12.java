package edu.umich.verdict.tpch;

import java.io.FileNotFoundException;

import edu.umich.verdict.BaseIT;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class TpchQuery12 {

    public static void main(String[] args) throws VerdictException, FileNotFoundException {
        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost(BaseIT.readHost());
        conf.setPort("21050");
        conf.setDbmsSchema("tpch1g");
        conf.set("loglevel", "debug");

        VerdictContext vc = VerdictJDBCContext.from(conf);
        String sql = "select\n" + 
                " l_shipmode,\n" + 
                " sum(case\n" + 
                "  when o_orderpriority = '1-URGENT'\n" + 
                "   or o_orderpriority = '2-HIGH'\n" + 
                "   then 1\n" + 
                "   else 0\n" + 
                "  end) as high_line_count,\n" + 
                "  sum(case\n" + 
                "   when o_orderpriority <> '1-URGENT'\n" + 
                "    and o_orderpriority <> '2-HIGH'\n" + 
                "     then 1\n" + 
                "   else 0\n" + 
                "   end) as low_line_count\n" + 
                "from\n" + 
                " orders,\n" + 
                " lineitem\n" + 
                "where\n" + 
                " o_orderkey = l_orderkey\n" + 
                " and l_shipmode in ('MAIL', 'SHIP')\n" + 
                " and l_commitdate < l_receiptdate\n" + 
                " and l_shipdate < l_commitdate\n" + 
                " and l_receiptdate >= '1994-01-01'\n" + 
                " and l_receiptdate < '1995-01-01'\n" + 
                "group by\n" + 
                " l_shipmode\n" + 
                "order by\n" + 
                " l_shipmode;\n";
        vc.executeJdbcQuery(sql);

        vc.destroy();
    }

}
