/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

public class TpchQuery2 {

    public static void main(String[] args) throws VerdictException, FileNotFoundException {
        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost(BaseIT.readHost());
        conf.setPort("21050");
        conf.setDbmsSchema("tpch1g");
        conf.set("loglevel", "debug");

        VerdictContext vc = VerdictJDBCContext.from(conf);
        String sql = "select\n" + " s_acctbal, s_name, n_name, p_partkey, p_mfgr,\n"
                + " s_address, s_phone, s_comment\n" + "from\n" + " part, supplier, partsupp, nation, region\n"
                + "where\n" + " p_partkey = ps_partkey\n" + " and s_suppkey = ps_suppkey\n" + " and p_size = 15\n"
                + " and p_type like '%BRASS'\n" + " and s_nationkey = n_nationkey\n"
                + " and n_regionkey = r_regionkey\n" + " and r_name = 'EUROPE'\n" + " and ps_supplycost = (\n"
                + "  select\n" + "   min(ps_supplycost)\n" + "  from\n" + "   partsupp,\n" + "   supplier,\n"
                + "   nation,\n" + "   region,\n" + "   part\n" + "  where\n" + "   p_partkey = ps_partkey\n"
                + "   and s_suppkey = ps_suppkey\n" + "   and s_nationkey = n_nationkey\n"
                + "   and n_regionkey = r_regionkey\n" + "   and r_name = 'EUROPE'\n" + "  )\n" + "order by\n"
                + " s_acctbal desc,\n" + " n_name,\n" + " s_name,\n" + " p_partkey\n" + "limit 100;\n";
        vc.executeJdbcQuery(sql);

        vc.destroy();
    }
}
