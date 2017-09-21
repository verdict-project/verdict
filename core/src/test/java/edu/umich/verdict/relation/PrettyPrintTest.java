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

import org.junit.Test;

import edu.umich.verdict.VerdictContext;

public class PrettyPrintTest {

    private static VerdictContext dummyContext = null;

    @Test
    public void selectAllTest() {
        String sql = "SELECT * FROM instacart1g.verdict_meta_size";
        System.out.println(Relation.prettyfySql(dummyContext, sql));
    }

    @Test
    public void partitionByTest() {
        String sql = "SELECT sum(price * (1 - discount)) OVER (partition by ship_method) FROM instacart1g.verdict_meta_size";
        System.out.println(Relation.prettyfySql(dummyContext, sql));
    }

    @Test
    public void stratifiedSampleTest() {
        String sql = "SELECT *, (count(*) OVER (partition by order_dow) / grp_size) AS verdict_sampling_prob "
                + "FROM (SELECT *, count(*) OVER (partition by order_dow) AS grp_size FROM orders) AS vt8 "
                + "WHERE rand(unix_timestamp()) < (3421082 * (0.01 / (grp_size / (SELECT count(distinct order_dow) AS expr1 FROM orders))))";
        System.out.println(Relation.prettyfySql(dummyContext, sql));
    }

    @Test
    public void ndvTest() {
        String sql = "select ndv(user_id) from orders;";
        System.out.println(Relation.prettyfySql(dummyContext, sql));
    }

}
