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

package edu.umich.verdict.impala;

import java.io.FileNotFoundException;

import edu.umich.verdict.BaseIT;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class ImpalaThroughVerdictTest {

    public static void main(String[] args) throws VerdictException, FileNotFoundException {

        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost(BaseIT.readHost());
        conf.setPort("21050");
        conf.set("loglevel", "debug");
        conf.set("verdict.meta_data.meta_database_suffix", "_verdict");
        conf.set("verdict.jdbc.schema", "instacart100g");

        VerdictContext vc = VerdictJDBCContext.from(conf);
        // vc.executeJdbcQuery("set verdict.meta_catalog_suffix=_verdict_impala");
        // vc.executeJdbcQuery("use tpch1g");
        // vc.executeJdbcQuery("select count(*) from instacart1g.orders");
        // vc.executeJdbcQuery("select count(*) from instacart1g.orders group by
        // order_hour_of_day");
        vc.executeJdbcQuery("select add_to_car_order, count(*)\n" + "from instacart100g.order_products\n"
                + "group by reordered, add_to_car_order\n" + "order by add_to_car_order");

        vc.destroy();
    }

}
