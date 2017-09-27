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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

import edu.umich.verdict.util.TypeCasting;

public class VerdictResultSetMetaData implements ResultSetMetaData {

    ResultSetMetaData meta;
    VerdictApproxResultMeta columnInfo;
    Map<Integer, Integer> columnMap;

    private int getMappedColumn(int i) {
        if (columnMap == null) {
            return i;
        } else {
            return columnMap.get(i);
        }
    }

    public VerdictResultSetMetaData(ResultSetMetaData meta, VerdictApproxResultMeta columnInfo) {
        this.meta = meta;
        this.columnInfo = columnInfo;
    }

    public VerdictResultSetMetaData(ResultSetMetaData meta, VerdictApproxResultMeta columnInfo,
            Map<Integer, Integer> columnMap) {
        this(meta, columnInfo);
        this.columnMap = columnMap;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return meta.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return meta.isWrapperFor(iface);
    }

    @Override
    public int getColumnCount() throws SQLException {
        if (columnMap != null)
            return columnMap.size();
        else
            return meta.getColumnCount();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return meta.isAutoIncrement(getMappedColumn(column));
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return meta.isCaseSensitive(getMappedColumn(column));
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return meta.isSearchable(getMappedColumn(column));
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return meta.isCurrency(getMappedColumn(column));
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return meta.isNullable(getMappedColumn(column));
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return meta.isSigned(getMappedColumn(column));
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return meta.getColumnDisplaySize(getMappedColumn(column));
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        if (columnInfo != null) {
            return columnInfo.getColumnLabel(getMappedColumn(column));
        } else {
            return meta.getColumnLabel(getMappedColumn(column));
        }
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumnLabel(column);
        // return meta.getColumnName(getMappedColumn(column));
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return meta.getSchemaName(getMappedColumn(column));
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return meta.getPrecision(getMappedColumn(column));
    }

    @Override
    public int getScale(int column) throws SQLException {
        return meta.getScale(getMappedColumn(column));
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return meta.getTableName(getMappedColumn(column));
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return meta.getCatalogName(getMappedColumn(column));
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return meta.getColumnType(getMappedColumn(column));
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return TypeCasting.dbDatatypeName(meta.getColumnType(getMappedColumn(column)));
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return meta.isReadOnly(getMappedColumn(column));
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return meta.isWritable(getMappedColumn(column));
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return meta.isDefinitelyWritable(getMappedColumn(column));
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return meta.getColumnClassName(getMappedColumn(column));
    }

}
