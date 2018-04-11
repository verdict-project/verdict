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

import java.util.ArrayList;
import java.util.List;

import org.antlr.runtime.BaseRecognizer;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.parser.VerdictSQLLexer;
import edu.umich.verdict.parser.VerdictSQLParser;
import org.antlr.v4.runtime.ConsoleErrorListener;
import org.antlr.v4.runtime.Token;

public class StringManipulations {

    public static int viewNameId = 0;

    /**
     * Returns an effective schema name of a table name specified in a query. For
     * instance, given "default.products", this method returns "default".
     * 
     * @param originalTableName
     * @return
     */
    public static Optional<String> schemaOfTableName(Optional<String> currentSchema, String originalTableName) {
        if (originalTableName == null)
            return currentSchema;

        String[] tokens = originalTableName.split("\\.");
        if (tokens.length > 1) {
            return Optional.fromNullable(tokens[0]);
        } else {
            return currentSchema;
        }
    }

    /**
     * Returns an effective schema name of a table name specified in a query. For
     * instance, given "default.products", this method returns "products".
     * 
     * @param originalTableName
     * @return
     */
    public static String tableNameOfTableName(String originalTableName) {
        if (originalTableName == null)
            return null;

        String[] tokens = originalTableName.split("\\.");
        if (tokens.length > 1) {
            return tokens[1];
        } else {
            return originalTableName;
        }
    }

    public static String colNameOfColName(String originalColName) {
        String[] tokens = originalColName.split("\\.");
        if (tokens.length > 1) {
            return tokens[tokens.length - 1];
        } else {
            return originalColName;
        }
    }

    public static String tabNameOfColName(String originalColName) {
        String[] tokens = originalColName.split("\\.");
        if (tokens.length > 1) {
            return tokens[tokens.length - 2];
        } else {
            return "";
        }
    }

    public static TableUniqueName tabUniqueNameOfColName(VerdictJDBCContext vc, String originalColName) {
        String[] tokens = originalColName.split("\\.");
        if (tokens.length > 2) {
            return TableUniqueName.uname(tokens[tokens.length - 3], tokens[tokens.length - 2]);
        } else if (tokens.length > 1) {
            return TableUniqueName.uname(vc, tokens[tokens.length - 2]);
        } else {
            return null;
        }
    }

    public static String attributeNameOfAttributeName(String originalAttrName) {
        String[] tokens = originalAttrName.split("\\.");
        if (tokens.length > 1) {
            return tokens[1];
        } else {
            return originalAttrName;
        }
    }

    /**
     * Returns a unique name for a table (including its schema name).
     * 
     * @param originalTableName
     * @return
     */
    public static String fullTableName(Optional<String> currentSchema, String originalTableName) {
        Optional<String> schema = schemaOfTableName(currentSchema, originalTableName);
        if (!schema.isPresent())
            return tableNameOfTableName(originalTableName);
        else
            return schema.get() + "." + tableNameOfTableName(originalTableName);
    }

    public static VerdictSQLParser parserOf(String text) {
        VerdictSQLLexer l = new VerdictSQLLexer(new ANTLRInputStream(text));
        l.removeErrorListeners();
        l.addErrorListener(new VerdictAntlrErrorListener());
        VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
        p.removeErrorListeners();
        p.addErrorListener(new VerdictAntlrErrorListener());
        return p;
    }

    public static List<String> quoteEveryString(List<String> list, String with) {
        List<String> quoted = new ArrayList<String>();
        for (String e : list) {
            quoted.add(quote(e, with));
        }
        return quoted;
    }

    private static String quote(String e, String with) {
        return String.format("%s%s%s", with, e.replace("\"", "").replace("`", "").replace("'", ""), with);
    }

    public static List<String> quoteString(List<Object> list, String with) {
        List<String> quoted = new ArrayList<String>();
        for (Object e : list) {
            if (e instanceof String) {
                quoted.add(quote(e.toString(), with));
            } else {
                quoted.add(e.toString());
            }
        }
        return quoted;
    }

    public static String stripQuote(String e) {
        return e.replace("`", "").replace("\"", "").replace("'", "");
    }

}
