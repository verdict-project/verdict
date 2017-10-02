/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.jdbc.impala;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.jdbc.MinimumTest;
import edu.umich.verdict.jdbc.TestBase;

public class ImpalaThroughVerdictJdbcTest extends TestBase {
    
    public static Connection conn;

    @BeforeClass
    public static void connect() throws VerdictException, SQLException, FileNotFoundException, ClassNotFoundException {
        Class.forName("edu.umich.verdict.jdbc.Driver");
        String host = readHost();
        String url = String.format("jdbc:verdict:impala://%s", host);
        conn = DriverManager.getConnection(url);
        
    }
    
    @Test
    @Category(MinimumTest.class)
    public void basicSelectCount() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("select count(*) from tpch1g.lineitem");
        stmt.close();
    }

    @AfterClass
    public static void destroy() throws SQLException {
        conn.close();
    }

}
