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

package edu.umich.verdict.relation;

import java.io.FileNotFoundException;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class SqlToApproxRelationUnitTest {

    static VerdictJDBCContext vc;

    @BeforeClass
    public static void connect() throws VerdictException, SQLException, FileNotFoundException {
        VerdictConf conf = new VerdictConf();
        conf.setHost("salat1.eecs.umich.edu");
        conf.setPort("21050");
        conf.setDbms("impala");
        conf.setDbmsSchema("instacart1g");
        conf.set("verdict.meta_catalog_suffix", "_verdict");
        vc = VerdictJDBCContext.from(conf);
    }

    @Test
    public void complexTest1() throws VerdictException {
        String sql = "SELECT 5*round(d1/5) as reorder_after_days, COUNT(*) "
                + "FROM (SELECT user_id, AVG(days_since_prior) AS d1, COUNT(*) AS c2 "
                + "      FROM order_products, orders " + "      WHERE orders.order_id = order_products.order_id "
                + "      AND days_since_prior IS NOT NULL " + "      GROUP BY user_id) t2 "
                + "WHERE c2 > (SELECT AVG(c1) AS a1 " + "            FROM (SELECT user_id, COUNT(*) AS c1 "
                + "                  FROM order_products, orders "
                + "                  WHERE orders.order_id = order_products.order_id "
                + "                  GROUP BY user_id) t1) " + "GROUP BY reorder_after_days "
                + "ORDER BY reorder_after_days";
        ExactRelation r = ExactRelation.from(vc, sql);
        System.out.println(r);
        ApproxRelation a = r.approx();
        System.out.println(a);
    }
}
