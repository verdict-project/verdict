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

import java.sql.ResultSet;
import java.util.Set;

import org.junit.Test;

import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class SampleIT extends TestBase {

    @Test
    public void showSampleTest() throws VerdictException {
        ResultSet rs = vc.executeJdbcQuery("show samples");
        ResultSetConversion.printResultSet(rs);
    }

    @Test
    public void createRecommendedSampleTest() throws VerdictException {
        vc.executeJdbcQuery("CREATE SAMPLE OF orders");
    }

    @Test
    public void createUniformSampleTest() throws VerdictException {
        vc.executeJdbcQuery("create uniform sample of orders");
    }

    @Test
    public void createStratifiedSampleTest() throws VerdictException {
        vc.executeJdbcQuery("create stratified sample of orders ON order_dow");
    }

    @Test
    public void createStratifiedSampleTest2() throws VerdictException {
        vc.executeJdbcQuery("create stratified sample of orders ON eval_set");
    }

    @Test
    public void createUniverseSampleTest() throws VerdictException {
        vc.executeJdbcQuery("CREATE UNIVERSE SAMPLE OF orders ON user_id");
    }

    @Test
    public void getColumnNamesTest() throws VerdictException {
        TableUniqueName orders = TableUniqueName.uname(vc, "orders");
        Set<String> columns = vc.getMeta().getColumns(orders);
        System.out.println(columns);
    }

    @Test
    public void dropRecommendedSampleTest() throws VerdictException {
        vc.executeJdbcQuery("DROP SAMPLE OF orders");
    }

}
