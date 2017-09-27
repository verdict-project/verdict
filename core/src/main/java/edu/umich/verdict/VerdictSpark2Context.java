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

package edu.umich.verdict;

import java.sql.ResultSet;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import edu.umich.verdict.dbms.DbmsSpark2;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.query.Query;
import edu.umich.verdict.util.VerdictLogger;

public class VerdictSpark2Context extends VerdictContext {

    private Dataset<Row> df;

    public VerdictSpark2Context(SparkContext sc) throws VerdictException {
        this(sc, new VerdictConf());
    }

    public VerdictSpark2Context(SparkContext sc, VerdictConf conf) throws VerdictException {
        super(conf);
        conf.setDbms("spark2");
        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        setDbms(new DbmsSpark2(this, sparkSession));
        setMeta(new VerdictMeta(this));
    }

    @Override
    public void execute(String sql) throws VerdictException {
        VerdictLogger.debug(this, "An input query:");
        VerdictLogger.debugPretty(this, sql, "  ");
        Query vq = Query.getInstance(this, sql);
        df = vq.computeDataset();
    }

    @Override
    public ResultSet getResultSet() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Dataset<Row> getDataset() {
        return df;
    }

    @Override
    public DataFrame getDataFrame() {
        return null;
    }
    
    public Dataset<Row> sql(String sql) throws VerdictException {
        return executeSpark2Query(sql);
    }
}
