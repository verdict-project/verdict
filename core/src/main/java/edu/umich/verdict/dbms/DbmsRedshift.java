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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
        return String.format("mod(strtol(crc32(cast(%s%s%s as text)),16),%d)",
                getQuoteString(), col, getQuoteString(), mod);
    }

    @Override
    public String modOfHash(List<String> columns, int mod) {
        String concatStr = "";
        for (int i = 0; i < columns.size(); ++i) {
            String col = columns.get(i);
            String castStr = String.format("cast(%s%s%s as text)", getQuoteString(), col, getQuoteString());
            if (i < columns.size() - 1) {
                castStr += String.format(",'%s',", HASH_DELIM);
            }
            concatStr += castStr;
        }
        return String.format("mod(strtol(crc32(concat(%s)),16),%d)", concatStr, mod);
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
    
    /**
     * Set the search_path of a Redshift. The search_path (or schemaName parameter) can be multiple strings
     * separated by commas.
     */
    @Override
    public void changeDatabase(String schemaName) throws VerdictException {
//        Set<String> existingSchemas = vc.getMeta().getDatabases();
//        String verdictMetaSchema = vc.getMeta().metaCatalogForDataCatalog(schemaName);
        setSearchPath(schemaName);
        String[] schemaList = schemaName.split(",");
        String primarySchema = schemaList[0];
        
//        if (existingSchemas.contains(verdictMetaSchema)) {
//            execute(String.format("set search_path=%s,%s", schemaName, verdictMetaSchema));
//        } else {
//            execute(String.format("set search_path=%s", schemaName));
//        }
        
        currentSchema = Optional.fromNullable(primarySchema);
        if (schemaList.length > 1) {
            VerdictLogger.info(String.format("Search path changed to: %s. For the tables speficied without their"
                    + " schemas, Verdict assumes that they are in a primary schema (%s). This limitation will be"
                    + " fixed in a future release.", schemaName, primarySchema));
        } else {
            VerdictLogger.info(String.format("Search path changed to: %s.", schemaName));
        }
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
    protected long attachUniformProbabilityToTempTable(SampleParam param, TableUniqueName temp)
            throws VerdictException {
        String samplingProbCol = vc.getDbms().samplingProbabilityColumnName();
        long total_size = SingleRelation.from(vc, param.getOriginalTable()).countValue();
        long sample_size = SingleRelation.from(vc, temp).countValue();

        ExactRelation withRand = SingleRelation.from(vc, temp).select("*, " + String
                .format("cast (%d as float) / cast (%d as float) as %s", sample_size, total_size, samplingProbCol));
        dropTable(param.sampleTableName());
        String sql = String.format("create table %s as %s", param.sampleTableName(), withRand.toSql());
        VerdictLogger.debug(this, "The query used for creating a temporary table without sampling probabilities:");
//        VerdictLogger.debugPretty(this, Relation.prettyfySql(vc, sql), "  ");
//        VerdictLogger.debug(this, sql);
        executeUpdate(sql);
        return sample_size;
    }

    @Override
    protected long createUniverseSampleWithProbFromSample(SampleParam param, TableUniqueName temp)
            throws VerdictException {
        String samplingProbCol = vc.getDbms().samplingProbabilityColumnName();
        ExactRelation sampled = SingleRelation.from(vc, temp);
        long total_size = SingleRelation.from(vc, param.getOriginalTable()).countValue();
        long sample_size = sampled.countValue();

        ExactRelation withProb = sampled
                .select(String.format("*, cast (%d as float)  / cast (%d as float) AS %s", sample_size, total_size,
                        samplingProbCol) + ", " + universePartitionColumn(param.getColumnNames().get(0)));

        String sql = String.format("create table %s AS %s", param.sampleTableName(), withProb.toSql());
        VerdictLogger.debug(this, "The query used for creating a universe sample with sampling probability:");
//        VerdictLogger.debugPretty(this, Relation.prettyfySql(vc, sql), "  ");
//        VerdictLogger.debug(this, sql);
        executeUpdate(sql);
        return sample_size;
    }

    @Override
    protected void createStratifiedSampleFromGroupSizeTemp(SampleParam param, TableUniqueName groupSizeTemp)
            throws VerdictException {
        Map<String, String> col2types = vc.getMeta().getColumn2Types(param.getOriginalTable());
        SampleSizeInfo info = vc.getMeta()
                .getSampleSizeOf(new SampleParam(vc, param.getOriginalTable(), "uniform", null, new ArrayList<String>()));
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
                        String.format("case when s.%s%s%s is null then '%s' else s.%s%s%s end",
                                getQuoteString(), col, getQuoteString(),
                                NULL_STRING,
                                getQuoteString(), col, getQuoteString()));
                Expr right = Expr.from(vc,
                        String.format("case when t.%s%s%s is null then '%s' else t.%s%s%s end",
                                getQuoteString(), col, getQuoteString(),
                                NULL_STRING,
                                getQuoteString(), col, getQuoteString()));
                joinExprs.add(Pair.of(left, right));
            } else if (isTimeStamp) {
                Expr left = Expr.from(vc,
                        String.format("case when s.%s%s%s is null then '%s' else s.%s%s%s end",
                                getQuoteString(), col, getQuoteString(),
                                NULL_TIMESTAMP,
                                getQuoteString(), col, getQuoteString()));
                Expr right = Expr.from(vc,
                        String.format("case when t.%s%s%s is null then '%s' else t.%s%s%s end",
                                getQuoteString(), col, getQuoteString(),
                                NULL_TIMESTAMP,
                                getQuoteString(), col, getQuoteString()));
                joinExprs.add(Pair.of(left, right));
            } else {
                Expr left = Expr.from(vc,
                        String.format("case when s.%s%s%s is null then %d else s.%s%s%s end", 
                                getQuoteString(), col, getQuoteString(),
                                NULL_LONG,
                                getQuoteString(), col, getQuoteString()));
                Expr right = Expr.from(vc,
                        String.format("case when t.%s%s%s is null then %d else t.%s%s%s end", 
                                getQuoteString(), col, getQuoteString(),
                                NULL_LONG,
                                getQuoteString(), col, getQuoteString()));
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
            selectElems.add(String.format("s.%s%s%s", getQuoteString(), col, getQuoteString()));
        }

        // sample table
        TableUniqueName sampledNoRand = Relation.getTempTableName(vc, param.sampleTableName().getSchemaName());
        ExactRelation sampled = SingleRelation.from(vc, param.getOriginalTable())
                .select(String.format("*, %s as %s", randomNumberExpression(param), randNumColname)).withAlias("s")
                .join(SingleRelation.from(vc, groupSizeTemp).withAlias("t"), joinExprs).where(whereClause)
                .select(Joiner.on(", ").join(selectElems) + ", " + groupSizeColName);
        String sql1 = String.format("create table %s as %s", sampledNoRand, sampled.toSql());
        VerdictLogger.debug(this, "The query used for creating a stratified sample without sampling probabilities.");
//        VerdictLogger.debugPretty(this, Relation.prettyfySql(vc, sql1), "  ");
        executeUpdate(sql1);

        // attach sampling probabilities and random partition number
        ExactRelation sampledGroupSize = SingleRelation.from(vc, sampledNoRand).groupby(param.getColumnNames())
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
//        VerdictLogger.debugPretty(this, Relation.prettyfySql(vc, sql2), "  ");
//        VerdictLogger.debug(this, sql2);
        executeUpdate(sql2);

        dropTable(sampledNoRand, false);
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
        String schemaName = tableUniqueName.getSchemaName();
        String tableName = tableUniqueName.getTableName();
        
        String search_path = getSearchPath();
        setSearchPath(schemaName);
        ResultSet rs = executeJdbcQuery(
                String.format("SELECT \"column\",\"type\" FROM pg_table_def WHERE tablename = '%s' AND schemaname = '%s'",
                              tableName, schemaName));
        setSearchPath(search_path);
        
        return rs;
    }
    
    private String getSearchPath() throws VerdictException {
        ResultSet rs = executeJdbcQuery("show search_path");
        List<String> searchList = new ArrayList<String>();
        try {
            while (rs.next()) {
                String[] schemaList = rs.getString(1).split(",");
                for (String a : schemaList) {
                    searchList.add(StringManipulations.stripQuote(a).trim());
                }
            }
        } catch (SQLException e) {
            throw new VerdictException(e);
        }
        String searchPath = Joiner.on(",").join(searchList);
        return searchPath;
    }
    
    private void setSearchPath(String search_path) throws VerdictException {
        List<String> quotedSchemaList = Arrays.asList(search_path.split(","));
        quotedSchemaList = StringManipulations.quoteEveryString(quotedSchemaList, getQuoteString());
        executeJdbcQuery(String.format("set search_path=%s", Joiner.on(",").join(quotedSchemaList)));
    }

    @Override
    public ResultSet getTablesInResultSet(String schema) throws VerdictException {
        String search_path = getSearchPath();
        setSearchPath(schema);
        ResultSet rs = executeJdbcQuery(String.format("SELECT DISTINCT tablename FROM pg_table_def WHERE schemaname = '%s'", schema));
        setSearchPath(search_path);
        return rs;
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