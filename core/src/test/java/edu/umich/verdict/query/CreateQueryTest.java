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

package edu.umich.verdict.query;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Scanner;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.umich.verdict.BaseIT;
import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class CreateQueryTest {

    protected static VerdictJDBCContext vc;

    @BeforeClass
    public static void connect() throws VerdictException, SQLException, FileNotFoundException {
        final String host = readHost();
        final String port = "21050";
        final String schema = "instacart1g";

        VerdictConf conf = new VerdictConf();
        conf.setDbms("impala");
        conf.setHost(host);
        conf.setPort(port);
        conf.setDbmsSchema(schema);
        conf.set("no_user_password", "true");
        vc = VerdictJDBCContext.from(conf);
    }

    @AfterClass
    public static void destroy() throws VerdictException {
        vc.destroy();
    }

    @Test
    public void externalTablTest() throws VerdictException {
        String sql = "create table mytable (col1 string, col2 double) " + "location hdfs://localhost:1000/data "
                + "fields separated by \",\" " + "escaped by \"\\\" ";
        // + "type csv "
        // + "quoted by \"\\\"\"";

        System.out.println(sql);
        vc.executeJdbcQuery(sql);

    }

    public static String readHost() throws FileNotFoundException {
        ClassLoader classLoader = BaseIT.class.getClassLoader();
        File file = new File(classLoader.getResource("integration_test_host.test").getFile());

        Scanner scanner = new Scanner(file);
        String line = scanner.nextLine();
        scanner.close();
        return line;
    }

}
