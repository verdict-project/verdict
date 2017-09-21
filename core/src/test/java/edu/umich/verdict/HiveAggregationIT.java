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

package edu.umich.verdict;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import edu.umich.verdict.exceptions.VerdictException;

@Category(IntegrationTest.class)
public class HiveAggregationIT extends AggregationIT {

    @BeforeClass
    public static void connect() throws VerdictException, SQLException, FileNotFoundException {
        final String host = readHost();
        final String port = "10000";
        final String schema = "instacart1g";

        VerdictConf conf = new VerdictConf();
        conf.setDbms("hive2");
        conf.setHost(host);
        conf.setPort(port);
        conf.setDbmsSchema(schema);
        conf.set("no_user_password", "true");
        vc = new VerdictJDBCContext(conf);

        String url = String.format("jdbc:hive2://%s:%s/%s", host, port, schema);
        Connection conn = DriverManager.getConnection(url);
        stmt = conn.createStatement();
    }

    @AfterClass
    public static void destroy() throws VerdictException {
        vc.destroy();
    }

    @Override
    public void simpleAvgUsingUniverseSample() throws VerdictException, SQLException {
        // TODO Auto-generated method stub
        super.simpleAvgUsingUniverseSample();
    }

    @Override
    public void simpleCountUsingUniverseSample() throws VerdictException, SQLException {
        // TODO Auto-generated method stub
        super.simpleCountUsingUniverseSample();
    }

    @Override
    public void groupbyCountDistinctUsingUniverseSample() throws VerdictException, SQLException {
        // TODO Auto-generated method stub
        super.groupbyCountDistinctUsingUniverseSample();
    }

    @Override
    public void groupbyCountDistinctUsingUniverseSample2() throws VerdictException, SQLException {
        // TODO Auto-generated method stub
        super.groupbyCountDistinctUsingUniverseSample2();
    }

}
