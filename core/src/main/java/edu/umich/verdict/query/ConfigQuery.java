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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.VerdictResultSet;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public class ConfigQuery extends SelectQuery {

    private String key;

    private String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public ConfigQuery(VerdictContext vc, String q) {
        super(vc, q);
        readKeyValueFromQuery();
    }

    protected void readKeyValueFromQuery() {
        if (getKey() != null)
            return;

        VerdictSQLParser p = StringManipulations.parserOf(queryString);

        VerdictSQLBaseVisitor<Pair<String, String>> visitor = new VerdictSQLBaseVisitor<Pair<String, String>>() {
            private Pair<String, String> keyValue;

            protected Pair<String, String> defaultResult() {
                return keyValue;
            }

            @Override
            public Pair<String, String> visitConfig_get_statement(VerdictSQLParser.Config_get_statementContext ctx) {
                keyValue = Pair.of(ctx.config_key().getText(), null);
                return keyValue;
            }

            @Override
            public Pair<String, String> visitConfig_set_statement(VerdictSQLParser.Config_set_statementContext ctx) {
                keyValue = Pair.of(ctx.config_key().getText(),
                        ctx.config_value().getText().replaceAll("^\"|\"$|^\'|\'$", ""));
                return keyValue;
            }
        };

        Pair<String, String> keyValue = visitor.visit(p.config_statement());
        setKey(keyValue.getKey());
        setValue(keyValue.getValue());
    }

    @Override
    public void compute() throws VerdictException {
        List<String> row = new ArrayList<String>();

        if (getValue() == null) {
            // get statement
            String value = vc.getConf().get(getKey());
            row.add(getKey());
            row.add(value);

            if (vc.getDbms().isJDBC()) {
                // To get a ResultSet, we temporarily create a table
                List<List<String>> data = new ArrayList<List<String>>();
                data.add(row);

                List<String> meta = new ArrayList<String>();
                meta.add("conf_key");
                meta.add("conf_value");
//                vc.getDbms().execute("select 1");

                rs = VerdictResultSet.fromList(data, meta);
            } else if (vc.getDbms().isSpark()) {
                VerdictLogger.warn(this, "DataFrame is not generated for Spark yet. The key-value pair is " + getKey()
                        + ": " + getValue());
            }
        } else {
            // set statement
            vc.getConf().set(getKey(), getValue());
            row.add(getKey());
            row.add(getKey());
        }
    }
}
