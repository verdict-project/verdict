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

public class TpchQuery6 {

    public static void main(String[] args) throws VerdictException, FileNotFoundException {
        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost(TestBase.readHost());
        conf.setPort("21050");
        conf.setDbmsSchema("tpch1g");
        conf.set("verdict.meta_data.meta_database_suffix", "_verdict_impala");
        conf.set("loglevel", "debug");

        VerdictContext vc = VerdictJDBCContext.from(conf);
        String sql = "select\n" + 
                " sum(l_extendedprice * l_discount) as revenue\n" + 
                "from\n" + 
                " lineitem\n" + 
                "where\n" + 
                " l_shipdate >= '1994-01-01'\n" + 
                " and l_shipdate < '1995-01-01'\n" + 
                " and l_discount between 0.05 and  0.07\n" + 
                " and l_quantity < 24;\n";
        vc.executeJdbcQuery(sql);

        vc.destroy();
    }
}
