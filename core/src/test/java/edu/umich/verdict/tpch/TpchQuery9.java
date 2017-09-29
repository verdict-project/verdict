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

import edu.umich.verdict.TestBase;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class TpchQuery9 {

    public static void main(String[] args) throws FileNotFoundException, VerdictException {
        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost(TestBase.readHost());
        conf.setPort("21050");
        conf.setDbmsSchema("tpch1g");
        conf.set("loglevel", "debug");

        VerdictContext vc = VerdictJDBCContext.from(conf);
        String sql = "select nation, o_year, sum(amount) as sum_profit\n" + "from (\n" + "  select\n"
                + "    n_name as nation,\n" + "    year(o_orderdate) as o_year,\n"
                + "    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\n" + "  from\n"
                + "    lineitem\n" + "    inner join orders on o_orderkey = l_orderkey\n"
                + "    inner join partsupp on ps_suppkey = l_suppkey\n"
                + "    inner join part on p_partkey = ps_partkey\n"
                + "    inner join supplier on s_suppkey = ps_suppkey\n"
                + "    inner join nation on s_nationkey = n_nationkey\n" + "  where p_name like '%green%') as profit\n"
                + "group by nation, o_year\n" + "order by nation, o_year desc";
        vc.executeJdbcQuery(sql);

        vc.destroy();
    }

}
