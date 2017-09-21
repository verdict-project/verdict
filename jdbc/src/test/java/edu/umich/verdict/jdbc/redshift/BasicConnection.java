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

package edu.umich.verdict.jdbc.redshift;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class BasicConnection {

    public static void main(String[] args) throws ClassNotFoundException, SQLException, VerdictException {

        Class.forName("edu.umich.verdict.jdbc.Driver");

        String url = "jdbc:verdict:redshift://verdict-redshift-demo.crc58e3qof3k.us-east-1.redshift.amazonaws.com:5439/dev;UID=admin;PWD=qKUcr2CUgSP3NjHE";
        Connection conn = DriverManager.getConnection(url);
        Statement statement = conn.createStatement();

        statement.executeQuery("use tpch100g_demo");

        statement.executeQuery("select extract(month from o_orderdate) as m,\n"
                + "       extract(day from o_orderdate),\n" + "       count(*)\n" + "from orders_lineitem\n"
                + "group by extract(month from o_orderdate),\n" + "         extract(day from o_orderdate)\n"
                + "order by m, extract(day from o_orderdate)\n" + "limit 10;\n");

    }

}
