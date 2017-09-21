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

package edu.umich.verdict.mysql;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class MySQLBootstrappingTest {

    public MySQLBootstrappingTest() {
        // TODO Auto-generated constructor stub
    }

    public static void main(String[] args) throws VerdictException {
        VerdictConf conf = new VerdictConf();
        conf.setDbms("mysql");
        conf.setHost("localhost");
        conf.setPort("3306");
        conf.setDbmsSchema("tpch1G");
        conf.setUser("verdict");
        conf.setPassword("verdict");
        VerdictJDBCContext vc = VerdictJDBCContext.from(conf);

        String sql = "select l_shipmode, count(*) from lineitem group by l_shipmode order by count(*) desc";

        String sql2 = "select l_shipmode, avg(l_discount) / avg(l_tax) from lineitem group by l_shipmode";

        String sql3 = "select l_shipdate, count(*) as R from lineitem group by l_shipdate order by R desc limit 10";

        ResultSet rs1 = vc.executeJdbcQuery(sql3);
        ResultSetConversion.printResultSet(rs1);

    }

}
