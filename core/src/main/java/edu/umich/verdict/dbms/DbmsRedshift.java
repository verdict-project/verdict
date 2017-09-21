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

package edu.umich.verdict.dbms;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.SampleSizeInfo;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.ExactRelation;
import edu.umich.verdict.relation.Relation;
import edu.umich.verdict.relation.SingleRelation;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public class DbmsRedshift extends DbmsJDBC {

    public DbmsRedshift(VerdictContext vc, String dbName, String host, String port, String schema, String user,
            String password, String jdbcClassName) throws VerdictException {
        super(vc, dbName, host, port, schema, user, password, jdbcClassName);
        currentSchema = Optional.of("public");
    }

    @Override
    public String getQuoteString() {
        return "\"";
    }

    @Override
    protected String modOfRand(int mod) {
        return String.format("RANDOM() %% %d", mod);
    }

    @Override
    public String modOfHash(String col, int mod) {
        return String.format("mod(strtol(crc32(cast(%s as text)),16),%d)", col, mod);
    }

    @Override
    protected String randomNumberExpression(SampleParam param) {
        String expr = "RANDOM()";
        return expr;
    }

    @Override
    public void createCatalog(String catalog) throws VerdictException {
        String sql = String.format("create schema if not exists %s", catalog);
        executeUpdate(sql);
    }

    @Override
    public void changeDatabase(String schemaName) throws VerdictException {
        Set<String> existingSchemas = vc.getMeta().getDatabases();
        String verdictMetaSchema = vc.getMeta().metaCatalogForDataCatalog(schemaName);

        if (existingSchemas.contains(verdictMetaSchema)) {
            execute(String.format("set search_path=%s,%s", schemaName, verdictMetaSchema));
        } else {
            execute(String.format("set search_path=%s", schemaName));
        }

        currentSchema = Optional.fromNullable(schemaName);
        VerdictLogger.info("Database changed to: " + schemaName);
    }

    @Override
    protected String randomPartitionColumn() {
        int pcount = partitionCount();
        return String.format("mod(cast(round(RANDOM()*%d) as integer), %d) AS %s", pcount, pcount,
                partitionColumnName());
    }

    @Override
    public void insertEntry(TableUniqueName tableName, List<Object> values) throws VerdictException {
        StringBuilder sql = new StringBuilder(1000);
        sql.append(String.format("insert into %s values ", tableName));
        sql.append("(");
        String with = "'";
        sql.append(Joiner.on(", ").join(StringManipulations.quoteString(values, with)));
        sql.append(")");
        executeUpdate(sql.toString());
    }

    /**
     * Includes casting to float
     */
    @Override
    protected void attachUniformProbabilityToTempTable(SampleParam param, TableUniqueName temp)
            throws VerdictException {
        String samplingProbCol = vc.getDbms().samplingProbabilityColumnName();
        long total_size = SingleRelation.from(vc, param.getOriginalTable()).countValue();
        long sample_size = SingleRelation.from(vc, temp).countValue();

        ExactRelation withRand = SingleRelation.from(vc, temp).select("*, " + String
                .format("cast (%d as float) / cast (%d as float) as %s", sample_size, total_size, samplingProbCol));
        dropTable(param.sampleTableName());
        String sql = String.format("create table %s as %s", param.sampleTableName(), withRand.toSql());
        VerdictLogger.debug(this, "The query used for creating a temporary table without sampling probabilities:");
        VerdictLogger.debugPretty(this, Relation.prettyfySql(vc, sql), "  ");
        VerdictLogger.debug(this, sql);
        executeUpdate(sql);
    }

    @Override
    protected void createUniverseSampleWithProbFromSample(SampleParam param, TableUniqueName temp)
            throws VerdictException {
        String samplingProbCol = vc.getDbms().samplingProbabilityColumnName();
        ExactRelation sampled = SingleRelation.from(vc, temp);
        long total_size = SingleRelation.from(vc, param.originalTable).countValue();
        long sample_size = sampled.countValue();

        ExactRelation withProb = sampled
                .select(String.format("*, cast (%d as float)  / cast (%d as float) AS %s", sample_size, total_size,
                        samplingProbCol) + ", " + universePartitionColumn(param.getColumnNames().get(0)));

        String parquetString = "";

        if (vc.getConf().areSamplesStoredAsParquet()) {
            parquetString = getParquetString();
        }

        String sql = String.format("create table %s%s AS %s", param.sampleTableName(), parquetString, withProb.toSql());
        VerdictLogger.debug(this, "The query used for creating a universe sample with sampling probability:");
        VerdictLogger.debugPretty(this, Relation.prettyfySql(vc, sql), "  ");
        VerdictLogger.debug(this, sql);
        executeUpdate(sql);
    }

    @Override
    protected void createStratifiedSampleFromGroupSizeTemp(SampleParam param, TableUniqueName groupSizeTemp)
            throws VerdictException {
        Map<String, String> col2types = vc.getMeta().getColumn2Types(param.originalTable);
        SampleSizeInfo info = vc.getMeta()
                .getSampleSizeOf(new SampleParam(vc, param.originalTable, "uniform", null, new ArrayList<String>()));
        long originalTableSize = info.originalTableSize;
        long groupCount = SingleRelation.from(vc, groupSizeTemp).countValue();
        String samplingProbColName = vc.getDbms().samplingProbabilityColumnName();

        // equijoin expression that considers possible null values
        List<Pair<Expr, Expr>> joinExprs = new ArrayList<Pair<Expr, Expr>>();
        for (String col : param.getColumnNames()) {
            boolean isString = false;
            boolean isTimeStamp = false;

            if (col2types.containsKey(col)) {
                if (col2types.get(col).toLowerCase().contains("char")
                        || col2types.get(col).toLowerCase().contains("str")) {
                    isString = true;
                } else if (col2types.get(col).toLowerCase().contains("time")) {
                    isTimeStamp = true;
                }
            }

            if (isString) {
                Expr left = Expr.from(vc,
                        String.format("case when s.%s is null then '%s' else s.%s end", col, NULL_STRING, col));
                Expr right = Expr.from(vc,
                        String.format("case when t.%s is null then '%s' else t.%s end", col, NULL_STRING, col));
                joinExprs.add(Pair.of(left, right));
            } else if (isTimeStamp) {
                Expr left = Expr.from(vc,
                        String.format("case when s.%s is null then '%s' else s.%s end", col, NULL_TIMESTAMP, col));
                Expr right = Expr.from(vc,
                        String.format("case when t.%s is null then '%s' else t.%s end", col, NULL_TIMESTAMP, col));
                joinExprs.add(Pair.of(left, right));
            } else {
                Expr left = Expr.from(vc,
                        String.format("case when s.%s is null then %d else s.%s end", col, NULL_LONG, col));
                Expr right = Expr.from(vc,
                        String.format("case when t.%s is null then %d else t.%s end", col, NULL_LONG, col));
                joinExprs.add(Pair.of(left, right));
            }
        }

        // where clause using rand function
        String whereClause = String.format("%s < %d * %f / %d / %s", randNumColname, originalTableSize,
                param.getSamplingRatio(), groupCount, groupSizeColName);

        // this should set to an appropriate variable.
        List<Pair<Integer, Double>> samplingProbForSize = vc.getConf().samplingProbabilitiesForStratifiedSamples();

        whereClause += String.format(" OR %s < (case", randNumColname);

        for (Pair<Integer, Double> sizeProb : samplingProbForSize) {
            int size = sizeProb.getKey();
            double prob = sizeProb.getValue();
            whereClause += String.format(" when %s >= %d then %f * %d / %s", groupSizeColName, size, prob, size,
                    groupSizeColName);
        }
        whereClause += " else 1.0 end)";

        // aliased select list
        List<String> selectElems = new ArrayList<String>();
        for (String col : col2types.keySet()) {
            selectElems.add(String.format("s.%s", col));
        }

        // sample table
        TableUniqueName sampledNoRand = Relation.getTempTableName(vc, param.sampleTableName().getSchemaName());
        ExactRelation sampled = SingleRelation.from(vc, param.getOriginalTable())
                .select(String.format("*, %s as %s", randomNumberExpression(param), randNumColname)).withAlias("s")
                .join(SingleRelation.from(vc, groupSizeTemp).withAlias("t"), joinExprs).where(whereClause)
                .select(Joiner.on(", ").join(selectElems) + ", " + groupSizeColName);
        String sql1 = String.format("create table %s as %s", sampledNoRand, sampled.toSql());
        VerdictLogger.debug(this, "The query used for creating a stratified sample without sampling probabilities.");
        VerdictLogger.debugPretty(this, Relation.prettyfySql(vc, sql1), "  ");
        executeUpdate(sql1);

        // attach sampling probabilities and random partition number
        ExactRelation sampledGroupSize = SingleRelation.from(vc, sampledNoRand).groupby(param.columnNames)
                .agg("count(*) AS " + groupSizeInSampleColName);
        ExactRelation withRand = SingleRelation.from(vc, sampledNoRand).withAlias("s")
                .join(sampledGroupSize.withAlias("t"), joinExprs).select(
                        Joiner.on(", ").join(selectElems)
                                + String.format(", cast(%s as float) / cast(%s as float) as %s",
                                        groupSizeInSampleColName, groupSizeColName, samplingProbColName)
                                + ", " + randomPartitionColumn());

        String parquetString = "";

        if (vc.getConf().areSamplesStoredAsParquet()) {
            parquetString = getParquetString();
        }

        String sql2 = String.format("create table %s%s as %s", param.sampleTableName(), parquetString,
                withRand.toSql());
        VerdictLogger.debug(this, "The query used for creating a stratified sample with sampling probabilities.");
        VerdictLogger.debugPretty(this, Relation.prettyfySql(vc, sql2), "  ");
        VerdictLogger.debug(this, sql2);
        executeUpdate(sql2);

        dropTable(sampledNoRand);
    }

    @Override
    String composeUrl(String dbms, String host, String port, String schema, String user, String password)
            throws VerdictException {
        StringBuilder url = new StringBuilder();
        url.append(String.format("jdbc:%s://%s:%s", dbms, host, port));

        if (schema != null) {
            url.append(String.format("/%s", schema));
        }

        if (!vc.getConf().ignoreUserCredentials() && user != null && user.length() != 0) {
            url.append(";");
            url.append(String.format("UID=%s", user));
        }
        if (!vc.getConf().ignoreUserCredentials() && password != null && password.length() != 0) {
            url.append(";");
            url.append(String.format("PWD=%s", password));
        }

        // pass other configuration options.
        for (Map.Entry<String, String> pair : vc.getConf().getConfigs().entrySet()) {
            String key = pair.getKey();
            String value = pair.getValue();

            if (key.startsWith("verdict") || key.equals("user") || key.equals("password")) {
                continue;
            }

            url.append(String.format(";%s=%s", key, value));
        }

        return url.toString();
    }

    @Override
    public ResultSet describeTableInResultSet(TableUniqueName tableUniqueName) throws VerdictException {
        return executeJdbcQuery(String.format(
                "SELECT \"column\",\"type\" FROM pg_table_def WHERE tablename = '%s' AND schemaname = '%s'",
                tableUniqueName.getTableName(), tableUniqueName.getSchemaName()));
    }

    @Override
    public ResultSet getTablesInResultSet(String schema) throws VerdictException {
        return executeJdbcQuery(
                String.format("SELECT DISTINCT tablename FROM pg_table_def WHERE schemaname = '%s'", schema));
    }

    /**
     * this actually gets the schemas instead of database since in redshift database
     * does not mater; schema matters.
     */
    @Override
    public ResultSet getDatabaseNamesInResultSet() throws VerdictException {
        // return executeJdbcQuery("select nspname from pg_namespace WHERE datistemplate
        // = false");
        return executeJdbcQuery("select nspname from pg_namespace");
    }

    @Override
    public void createMetaTablesInDMBS(TableUniqueName originalTableName, TableUniqueName sizeTableName,
            TableUniqueName nameTableName) throws VerdictException {
        VerdictLogger.debug(this, "Creates meta tables if not exist.");
        String sql = String.format("CREATE TABLE IF NOT EXISTS %s", sizeTableName) + " (schemaname VARCHAR(120), "
                + " tablename VARCHAR(120), " + " samplesize BIGINT, " + " originaltablesize BIGINT)";
        executeUpdate(sql);

        sql = String.format("CREATE TABLE IF NOT EXISTS %s", nameTableName) + " (originalschemaname VARCHAR(120), "
                + " originaltablename VARCHAR(120), " + " sampleschemaaname VARCHAR(120), "
                + " sampletablename VARCHAR(120), " + " sampletype VARCHAR(120), " + " samplingratio FLOAT, "
                + " columnnames VARCHAR(120))";
        executeUpdate(sql);

        VerdictLogger.debug(this, "Meta tables created.");
    }

    @Override
    public Dataset<Row> getDataset() {
        // TODO Auto-generated method stub
        return null;
    }

}