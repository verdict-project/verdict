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

package edu.umich.verdict.jdbc.mysql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;

import edu.umich.verdict.exceptions.VerdictException;

public class MySQLDefaultTest {

    public MySQLDefaultTest() {
        // TODO Auto-generated constructor stub
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, VerdictException {

        Class.forName("com.mysql.jdbc.Driver");

        String url = "jdbc:mysql://localhost:3306/verdict?user=verdict&password=verdict";
        Connection conn = DriverManager.getConnection(url);
        DatabaseMetaData databaseMetaData = conn.getMetaData();

        ResultSet rs = databaseMetaData.getColumns("verdict", null, null, null);
        // ResultSet rs = databaseMetaData.getTables("verdict", null, null, null);
        // ResultSetConversion.printResultSet(rs);

        System.out.println("hello");
        SQLWarning warning = conn.getWarnings().getNextWarning();
        while (warning != null) {
            System.out.println(String.format("State: %s, Error: %s, Msg: %s", warning.getSQLState(),
                    warning.getErrorCode(), warning.getMessage()));
            warning = warning.getNextWarning();
        }

    }

}
