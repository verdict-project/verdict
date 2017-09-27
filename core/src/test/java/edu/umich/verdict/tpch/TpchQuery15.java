/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        String sql0 = "select l_suppkey as supplier_no,\n"
                + "       sum(l_extendedprice * (1 - l_discount)) as total_revenue\n" + "from lineitem\n"
                + "where l_shipdate >= '1996-01-01' and l_shipdate < '1996-04-01'\n" + "group by l_suppkey\n"
                + "limit 10;\n";

        String sql1 = "create view revenue_temp as\n" + "select l_suppkey as supplier_no,\n"
                + "       sum(l_extendedprice * (1 - l_discount)) as total_revenue\n" + "from lineitem\n"
                + "where l_shipdate >= '1996-01-01' and l_shipdate < '1996-04-01'\n" + "group by l_suppkey;\n";

        String sql2 = "select s_suppkey, s_name, s_address, s_phone, total_revenue\n" + "from supplier, revenue_temp\n"
                + "where s_suppkey = supplier_no\n" + "  and total_revenue = (\n"
                + "        select max(total_revenue)\n" + "        from revenue_temp)\n" + "order by s_suppkey;\n";

        String sql3 = "drop view revenue_temp;\n";

        vc.executeJdbcQuery(sql0);

        vc.destroy();
    }

}
