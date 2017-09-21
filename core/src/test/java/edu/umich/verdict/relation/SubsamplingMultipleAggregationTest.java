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

import java.sql.ResultSet;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class SubsamplingMultipleAggregationTest {

    public static void main(String[] args) throws VerdictException {
        VerdictConf conf = new VerdictConf();
        conf.setHost("salat1.eecs.umich.edu");
        conf.setDbms("impala");
        conf.setPort("21050");
        conf.setDbmsSchema("instacart1g");
        conf.set("no_user_password", "true");
        VerdictJDBCContext vc = VerdictJDBCContext.from(conf);

        String sql;
        ExactRelation r;
        String converted;
        ResultSet rs;

        // sql = "select count(distinct user_id), count(*) from orders";
        // rs = vc.executeQuery(sql);
        // ResultSetConversion.printResultSet(rs);

        // sql = "select count(distinct user_id), count(distinct order_id) from orders";
        // rs = vc.executeQuery(sql);
        // ResultSetConversion.printResultSet(rs);

        // sql = "select count(distinct user_id), count(distinct order_id), count(*)
        // from orders";
        // rs = vc.executeQuery(sql);
        // ResultSetConversion.printResultSet(rs);

        // sql = "select order_dow, count(distinct user_id), count(*) from orders group
        // by order_dow order by order_dow";
        // rs = vc.executeQuery(sql);
        // ResultSetConversion.printResultSet(rs);

        // sql = "select order_dow AS dow, count(distinct user_id), count(distinct
        // order_id), count(*) from orders group by order_dow order by order_dow";
        // rs = vc.executeQuery(sql);
        // ResultSetConversion.printResultSet(rs);

        sql = "select dow AS dow1 from ("
                + "select order_dow AS dow, count(distinct user_id), count(distinct order_id), count(*) "
                + "from orders " + "group by order_dow " + "order by order_dow) t1";
        rs = vc.executeJdbcQuery(sql);
        ResultSetConversion.printResultSet(rs);

        vc.destroy();
    }

}
