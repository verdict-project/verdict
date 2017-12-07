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

package edu.umich.verdict.query;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.dbms.DbmsJDBC;
//import edu.umich.verdict.dbms.DbmsSpark;
import edu.umich.verdict.dbms.DbmsSpark2;
import edu.umich.verdict.exceptions.VerdictException;

public class ShowDatabasesQuery extends SelectQuery {

    public ShowDatabasesQuery(VerdictContext vc, String q) {
        super(vc, q);
    }

    @Override
    public void compute() throws VerdictException {
        if (vc.getDbms() instanceof DbmsJDBC) {
            rs = ((DbmsJDBC) vc.getDbms()).getDatabaseNamesInResultSet();
//        } else if (vc.getDbms() instanceof DbmsSpark) {
//            df = ((DbmsSpark) vc.getDbms()).getDatabaseNamesInDataFrame();
        } else if (vc.getDbms() instanceof DbmsSpark2) {
            ds = ((DbmsSpark2) vc.getDbms()).getDatabaseNamesInDataset();
        }
    }

}
