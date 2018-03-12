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

package edu.umich.verdict.dbms;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.exceptions.VerdictException;

public class DbmsImpala extends DbmsJDBC {

    public DbmsImpala(VerdictContext vc, String dbName, String host, String port, String schema, String user,
            String password, String jdbcClassName) throws VerdictException {
        super(vc, dbName, host, port, schema, user, password, jdbcClassName);
    }

    @Override
    public String modOfHash(String col, int mod) {
        return String.format("abs(fnv_hash(cast(%s%s%s AS STRING))) %% %d",
                getQuoteString(), col, getQuoteString(), mod);
    }

    @Override
    public String modOfHash(List<String> columns, int mod) {
        String concatStr = "";
        for (int i = 0; i < columns.size(); ++i) {
            String col = columns.get(i);
            String castStr = String.format("cast(%s%s%s AS STRING)", getQuoteString(), col, getQuoteString());
            if (i < columns.size() - 1) {
                castStr += ",";
            }
            concatStr += castStr;
        }
        return String.format("abs(fnv_hash((concat_ws('%s', %s)))) %% %d", HASH_DELIM, concatStr, mod);
    }

    @Override
    public String modOfRand(int mod) {
        return String.format("abs(rand(unix_timestamp())) %% %d", mod);
    }

    @Override
    protected String randomNumberExpression(SampleParam param) {
        Map<String, String> col2types = vc.getMeta().getColumn2Types(param.getOriginalTable());
        Set<String> hashCols = new HashSet<String>();
        int precision = 3;
        int modValue = (int) Math.pow(10, precision);

        for (Map.Entry<String, String> col2type : col2types.entrySet()) {
            String col = col2type.getKey();
            String type = col2type.getValue();
            if (type.toLowerCase().contains("char") || type.toLowerCase().contains("str")) {
                hashCols.add(String.format(
                        "fnv_hash((case when %s is null then cast(unix_timestamp() as string) else %s end))", col,
                        col));
            } else if (type.toLowerCase().contains("time")) {
                hashCols.add(String.format("fnv_hash((case when %s is null then current_timestamp() else %s end))", col,
                        col));
            } else {
                hashCols.add(
                        String.format("fnv_hash((case when %s is null then unix_timestamp() else %s end))", col, col));
            }
        }
        String expr = "abs(fnv_hash(" + Joiner.on(" + ").join(hashCols)
                + String.format(" + unix_timestamp())) %% %d / %d", modValue, modValue);
        return expr;
    }

    protected String randomPartitionColumn() {
        int pcount = partitionCount();
        return String.format("round(rand(unix_timestamp())*%d) %% %d AS %s", pcount, pcount, partitionColumnName());
    }

    @Override
    public Dataset<Row> getDataset() {
        // TODO Auto-generated method stub
        return null;
    }

}
