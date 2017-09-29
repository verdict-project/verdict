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

package edu.umich.verdict;

import java.sql.SQLException;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import edu.umich.verdict.exceptions.VerdictException;

public class MiscIT extends TestBase {

    protected void runSql(String sql) throws VerdictException {
        vc.executeJdbcQuery(sql);
    }

    @Test
    public void showDatabases() throws VerdictException {
        String sql = "show databases";
        runSql(sql);
    }

    @Test
    public void showTables() throws VerdictException {
        String sql = "show tables";
        runSql(sql);
    }

    @Test
    public void describeTables() throws VerdictException {
        String sql = "describe orders";
        runSql(sql);
    }

    @Test
    public void getColumnsTest() throws VerdictException, SQLException {
        List<Pair<String, String>> tabCols = vc.getDbms().getAllTableAndColumns("instacart1g");
        for (Pair<String, String> tabCol : tabCols) {
            System.out.println(tabCol.getLeft() + " " + tabCol.getRight());
        }
    }
}
