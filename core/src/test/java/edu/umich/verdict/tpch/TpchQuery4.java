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

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

import java.io.FileNotFoundException;

public class TpchQuery4 {

    public static void main(String[] args) throws VerdictException, FileNotFoundException {
        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost("salat3.eecs.umich.edu");
        conf.setPort("21050");
        conf.setDbmsSchema("tpch1g");
        conf.set("loglevel", "debug");

        VerdictContext vc = VerdictJDBCContext.from(conf);
        String sql = "select\n" +
                " o_orderpriority,\n" +
                " count(*) as order_count\n" +
                "from\n" +
                " orders\n" +
                "where\n" +
                " o_orderdate >= '1993-07-01'\n" +
                " and o_orderdate < '1993-10-01'\n" +
                " and exists (\n" +
                "  select\n" +
                "   *\n" +
                "  from\n" +
                "   lineitem\n" +
                "  where\n" +
                "   l_orderkey = o_orderkey\n" +
                "   and l_commitdate < l_receiptdate\n" +
                "  )\n" +
                "group by\n" +
                " o_orderpriority\n" +
                "order by\n" +
                " o_orderpriority";
//        String sql = "select\n" +
//                "*\n" +
//                "from\n" +
//                " orders\n" +
//                "where\n" +
//                "exists (\n" +
//                "  select\n" +
//                "   *\n" +
//                "  from\n" +
//                "   lineitem\n" +
//                ")";
//        String sql ="select\n" +
//                "*\n" +
//                "from\n" +
//                " orders\n" +
//                "where\n" +
//                "orders.a = 1";


        vc.executeJdbcQuery(sql);

        vc.destroy();
    }

}
