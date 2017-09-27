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

package edu.umich.verdict.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ResultSetConversion {

    public static String resultSetToString(ResultSet rs) {
        StringBuilder s = new StringBuilder(1000);

        try {
            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();

            for (int j = 1; j <= cols; j++) {
                s.append(String.format("[%s, %s]", meta.getColumnTypeName(j), meta.getColumnLabel(j)) + "\t");
            }
            s.append("\n");

            while (rs.next()) {
                for (int j = 1; j <= cols; j++) {
                    s.append(rs.getObject(j) + "\t");
                }
                s.append("\n");
            }

        } catch (SQLException e) {
            s.append(StackTraceReader.stackTrace2String(e));
        }

        return s.toString();
    }

    public static void printResultSet(ResultSet rs) {
        System.out.println(resultSetToString(rs));
    }

    public static List<Object> firstColumn(List<List<Object>> o) {
        List<Object> c = new ArrayList<Object>();
        for (List<Object> l : o) {
            c.add(l.get(0));
        }
        return c;
    }

}
