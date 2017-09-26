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

package edu.umich.verdict.hive;

import java.io.FileNotFoundException;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import edu.umich.verdict.IntegrationTest;
import edu.umich.verdict.SampleIT;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

@Category(IntegrationTest.class)
public class HiveSampleIT extends SampleIT {

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
        conn = DriverManager.getConnection(url);
        stmt = conn.createStatement();
    }

    @Override
    public void createStratifiedSampleTest() throws VerdictException {
        super.createStratifiedSampleTest();
    }

    @AfterClass
    public static void destroy() throws VerdictException, SQLException {
        stmt.close();
        conn.close();
        vc.destroy();
    }

    @Override
    public void createUniverseSampleTest() throws VerdictException {
        super.createUniverseSampleTest();
    }

    @Override
    public void createRecommendedSampleTest() throws VerdictException {
        super.createRecommendedSampleTest();
    }

    @Override
    public void getColumnNamesTest() throws VerdictException {
        super.getColumnNamesTest();
    }

    @Override
    public void dropRecommendedSampleTest() throws VerdictException {
        super.dropRecommendedSampleTest();
    }

    @Override
    public void createUniformSampleTest() throws VerdictException {
        // TODO Auto-generated method stub
        super.createUniformSampleTest();
    }

}
