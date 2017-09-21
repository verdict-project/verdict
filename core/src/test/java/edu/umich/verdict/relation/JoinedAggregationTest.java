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

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class JoinedAggregationTest {

    public static void main(String[] args) throws VerdictException {
        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setPort("21050");
        conf.setDbmsSchema("instacart1g");
        conf.set("no_user_password", "true");
        VerdictJDBCContext vc = VerdictJDBCContext.from(conf);

        SingleRelation r1 = SingleRelation.from(vc, "order_products");
        SingleRelation r2 = SingleRelation.from(vc, "orders");
        SingleRelation r3 = SingleRelation.from(vc, "products");
        System.out.println(r1.join(r2).where("order_products.order_id = orders.order_id").join(r3)
                .where("order_products.product_id = products.product_id").groupby("product_name")
                .approxAgg("count(*) as order_count").orderby("order_count desc").limit(10).collectAsString());
        // System.out.println(
        // r1.join(r2).where("order_products.order_id = orders.order_id")
        // .groupby("order_dow").approxCounts()
        // .collectAsString());
        // System.out.println(r1.join(r2).where("order_products.order_id =
        // orders.order_id").approxCountDistinct("user_id"));
        // System.out.println(r1.join(r2).where("order_products.order_id =
        // orders.order_id").count());

        vc.destroy();
    }

}
