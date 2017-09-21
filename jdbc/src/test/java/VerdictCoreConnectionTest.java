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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class VerdictCoreConnectionTest {

    public static void main(String args[]) throws ClassNotFoundException, SQLException {

        Class.forName("edu.umich.verdict.jdbc.Driver");

        String url = "jdbc:verdict:impala://localhost:3306/hue";
        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();

        String sql = "select * from table";
        ResultSet rs = stmt.executeQuery(sql);

        while (rs.next()) {
            int cols = rs.getMetaData().getColumnCount();

            for (int i = 0; i < cols; i++) {
                System.out.print(rs.getString(i) + " ");
            }
            System.out.println();
        }

        rs.close();
        stmt.close();
        conn.close();
    }

}
