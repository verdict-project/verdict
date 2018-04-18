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

package edu.umich.verdict.datatypes;

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.util.StringManipulations;

import java.io.Serializable;

public class TableUniqueName implements Serializable, Comparable<TableUniqueName> {

    private static final long serialVersionUID = 1L;

    private final String schemaName;

    private final String tableName;

    public String getDatabaseName() {
        return getSchemaName();
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public TableUniqueName(String schemaName, String tableName) {
        this.schemaName = (schemaName != null) ? schemaName.toLowerCase() : schemaName;
        this.tableName = (tableName != null) ? tableName.toLowerCase() : tableName;
    }

    public static TableUniqueName uname(String schemaName, String tableName) {
        return new TableUniqueName(schemaName, tableName);
    }

    public static TableUniqueName uname(VerdictContext vc, String tableName) {
        Optional<String> schema = StringManipulations.schemaOfTableName(vc.getCurrentSchema(), tableName);
        String table = StringManipulations.tableNameOfTableName(tableName);
        if (schema.isPresent()) {
            return new TableUniqueName(schema.get(), table);
        } else {
            return new TableUniqueName(null, table);
        }
    }

    public String fullyQuantifiedName() {
        return fullyQuantifiedName(schemaName, tableName);
    }

    public static String fullyQuantifiedName(String schema, String table) {
        return StringManipulations.fullTableName(Optional.fromNullable(schema), table);
    }

    @Override
    public int hashCode() {
        if (schemaName == null) {
            return tableName.hashCode();
        } else {
            return schemaName.hashCode() + tableName.hashCode();
        }
    }

    @Override
    public boolean equals(Object another) {
        if (another instanceof TableUniqueName) {
            TableUniqueName t = (TableUniqueName) another;
            if (schemaName == null && t.getSchemaName() == null) {
                return tableName.equals(t.tableName);
            } else if (schemaName != null && t.getSchemaName() != null) {
                return schemaName.equals(t.getSchemaName()) && tableName.equals(t.getTableName());
            }
            return false;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return fullyQuantifiedName(schemaName, tableName);
    }

    public int compareTo(TableUniqueName another) {
        if (this.schemaName.compareTo(another.schemaName) == 0) {
            return this.tableName.compareTo(another.tableName);
        } else {
            return this.schemaName.compareTo(another.schemaName);
        }
    }
}