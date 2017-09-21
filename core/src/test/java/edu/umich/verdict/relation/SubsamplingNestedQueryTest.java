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

public class SubsamplingNestedQueryTest {

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

        sql = "select avg(days_since_prior) from orders where days_since_prior > (select avg(days_since_prior) from orders)";
        r = ExactRelation.from(vc, sql);
        rs = vc.executeJdbcQuery(sql);
        ResultSetConversion.printResultSet(rs);
    }

}
