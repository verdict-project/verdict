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

package edu.umich.verdict.demo;

import java.io.FileNotFoundException;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class instacart {

    static VerdictJDBCContext vc;

    @BeforeClass
    public static void connect() throws VerdictException, SQLException, FileNotFoundException {
        VerdictConf conf = new VerdictConf();
        conf.setHost("salat1.eecs.umich.edu");
        conf.setPort("21050");
        conf.setDbms("impala");
        conf.setDbmsSchema("instacart100g");
        conf.set("verdict.meta_catalog_suffix", "_verdict");
        vc = VerdictJDBCContext.from(conf);
    }

    @Test
    public void test() throws VerdictException {

        String sql = "SELECT product_name, count(*) as order_count "
                + "FROM instacart100g.order_products, instacart100g.orders, instacart100g.products "
                + "WHERE orders.order_id = order_products.order_id "
                + "  AND order_products.product_id = products.product_id " + "  AND (order_dow = 0 OR order_dow = 1) "
                + "GROUP BY product_name " + "ORDER BY order_count DESC " + "LIMIT 5";
        vc.sql(sql);
    }

}
