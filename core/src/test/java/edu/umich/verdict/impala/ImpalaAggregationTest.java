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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import edu.umich.verdict.AggregationIT;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class ImpalaAggregationTest extends AggregationIT {

    @BeforeClass
    public static void connect() throws VerdictException, SQLException, FileNotFoundException, ClassNotFoundException {
        final String host = readHost();
        final String port = "21050";
        final String schema = "instacart1g";

        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost(host);
        conf.setPort(port);
        conf.setDbmsSchema(schema);
        conf.set("verdict.loglevel", "debug");
        conf.set("verdict.meta_data.meta_database_suffix", "_verdict");
        vc = VerdictJDBCContext.from(conf);

        String url = String.format("jdbc:impala://%s:%s/%s", host, port, schema);
        Class.forName("com.cloudera.impala.jdbc41.Driver");
        Connection conn = DriverManager.getConnection(url);
        stmt = conn.createStatement();
    }

    @AfterClass
    public static void destroy() throws VerdictException {
        vc.destroy();
    }

    @Override
    public void simpleCountDistinctUsingUniverseSample() throws VerdictException, SQLException {
        // TODO Auto-generated method stub
        super.simpleCountDistinctUsingUniverseSample();
    }

    @Override
    public void groupbySum2() throws VerdictException, SQLException {
        // TODO Auto-generated method stub
        super.groupbySum2();
    }
}
