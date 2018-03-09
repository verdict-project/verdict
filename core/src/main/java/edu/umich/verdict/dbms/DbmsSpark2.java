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

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import edu.umich.verdict.VerdictConf;
import org.apache.commons.lang3.tuple.Pair;
//import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public class DbmsSpark2 extends Dbms {

    private static String DBNAME = "spark2";

    //protected SQLContext sqlContext;

    protected SparkSession sparkSession;

    protected Dataset<Row> df;

    protected Set<TableUniqueName> cachedTable;

    protected VerdictConf conf;

    public DbmsSpark2(VerdictContext vc, SparkSession sparkSession, VerdictConf conf) throws VerdictException {
        super(vc, DBNAME);

        Optional<String> schema = Optional.of(conf.getDbmsSchema());
        setCurrentSchema(schema);

        this.sparkSession = sparkSession;
        this.cachedTable = new HashSet<TableUniqueName>();
        this.conf = conf;
    }

    public Dataset<Row> getDatabaseNamesInDataset() throws VerdictException {
        Dataset<Row> df = executeSpark2Query("show databases");
        return df;
    }

    public Dataset<Row> getTablesInDataset(String schemaName) throws VerdictException {
        Dataset<Row> df = executeSpark2Query("show tables in " + schemaName);
        return df;
    }

    public Dataset<Row> describeTableInDataset(TableUniqueName tableUniqueName) throws VerdictException {
        Dataset<Row> df = executeSpark2Query(String.format("describe %s", tableUniqueName));
        return df;
    }

    @Override
    public boolean execute(String sql) throws VerdictException {
        VerdictLogger.debug(this, "About to run: " + sql);
        df = sparkSession.sql(sql);
        return (df != null) ? true : false;
        // return (df.count() > 0)? true : false;
    }

    @Override
    public void executeUpdate(String sql) throws VerdictException {
        execute(sql);
    }

    @Override
    public ResultSet getResultSet() {
        return null;
    }

    @Override
    public Dataset<Row> getDataset() {
        return df;
    }

//    @Override
//    public DataFrame getDataFrame() {
//        return null;
//    }

    public Dataset<Row> emptyDataset() {
        return sparkSession.emptyDataFrame();
    }

    @Override
    public Set<String> getDatabases() throws VerdictException {
        Set<String> databases = new HashSet<String>();
        List<Row> rows = getDatabaseNamesInDataset().collectAsList();
        for (Row row : rows) {
            String dbname = row.getString(0);
            databases.add(dbname);
        }
        return databases;
    }

    @Override
    public List<String> getTables(String schema) throws VerdictException {
        List<String> tables = new ArrayList<String>();
        List<Row> rows = getTablesInDataset(schema).collectAsList();
        for (Row row : rows) {
            String table = row.getString(1);
            tables.add(table);
        }
        return tables;
    }

    @Override
    public long getTableSize(TableUniqueName tableName) throws VerdictException {
        String sql = String.format("select count(*) from %s", tableName);
        Dataset<Row> df = executeSpark2Query(sql);
        long size = df.collectAsList().get(0).getLong(0);
        return size;
    }

    @Override
    public Map<String, String> getColumns(TableUniqueName table) throws VerdictException {
        Map<String, String> col2type = new LinkedHashMap<String, String>();
        List<Row> rows = describeTableInDataset(table).collectAsList();
        for (Row row : rows) {
            String column = row.getString(0);
            if (column.substring(0,1).equals("#")) {
                break;
            }
            String type = row.getString(1);
            col2type.put(column, type);
        }
        return col2type;
    }

    @Override
    public void deleteEntry(TableUniqueName tableName, List<Pair<String, String>> colAndValues)
            throws VerdictException {
        VerdictLogger.warn(this, "deleteEntry() not implemented for DbmsSpark");
    }

    @Override
    public void insertEntry(TableUniqueName tableName, List<Object> values) throws VerdictException {
        StringBuilder sql = new StringBuilder(1000);
        sql.append(String.format("insert into %s ", tableName));
        sql.append("select t.* from (select ");
        String with = "'";
        sql.append(Joiner.on(", ").join(StringManipulations.quoteString(values, with)));
        sql.append(") t");
        executeUpdate(sql.toString());
    }

    @Override
    public void cacheTable(TableUniqueName tableName) {
        if (vc.getConf().cacheSparkSamples() && !cachedTable.contains(tableName)) {
            sparkSession.sql(String.format("cache table %s", tableName.toString()));
            cachedTable.add(tableName);
        }
    }

    @Override
    public String modOfHash(String col, int mod) {
        return String.format("crc32(cast(%s%s%s as string)) %% %d", getQuoteString(), col, getQuoteString(), mod);
    }

    @Override
    public String modOfHash(List<String> columns, int mod) {
        String concatStr = "";
        for (int i = 0; i < columns.size(); ++i) {
            String col = columns.get(i);
            String castStr = String.format("cast(%s%s%s as string)", getQuoteString(), col, getQuoteString());
            if (i < columns.size() - 1) {
                castStr += ",";
            }
            concatStr += castStr;
        }
        return String.format("crc32(concat_ws('%s', %s)) %% %d", HASH_DELIM, concatStr, mod);
    }

    @Override
    protected String randomPartitionColumn() {
        int pcount = partitionCount();
        return String.format("round(rand(unix_timestamp())*%d) %% %d AS %s", pcount, pcount, partitionColumnName());
    }

    @Override
    protected String randomNumberExpression(SampleParam param) {
        String expr = "rand(unix_timestamp())";
        return expr;
    }

    @Override
    public boolean isSpark() {
        return false;
    }

    @Override
    public boolean isSpark2() {
        return true;
    }

    @Override
    public void close() throws VerdictException {
        // TODO Auto-generated method stub
    }

    @Override
    protected String modOfRand(int mod) {
        return String.format("pmod(abs(rand(unix_timestamp())), %d)", mod);
    }

}
