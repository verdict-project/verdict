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

package edu.umich.verdict;

import java.sql.ResultSet;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import edu.umich.verdict.dbms.DbmsSpark;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.query.Query;
import edu.umich.verdict.util.VerdictLogger;

/**
 * Issues queries through Spark's HiveContext. Supports Spark 1.6.
 * @author Yongjoo Park
 *
 */
public class VerdictSparkHiveContext extends VerdictContext {

    private DataFrame df;
    
    private HiveContext sqlContext;

    public VerdictSparkHiveContext(SparkContext sc) throws VerdictException {
        this(sc, new VerdictConf());
    }

    public VerdictSparkHiveContext(SparkContext sc, VerdictConf conf) throws VerdictException {
        super(conf);
        conf.setDbms("spark");
        sqlContext = new HiveContext(sc);
        setDbms(new DbmsSpark(this, sqlContext));
        setMeta(new VerdictMeta(this));
    }
    
    /**
     * Returns the underlying regular HiveContext
     * @return
     */
    public HiveContext getHiveContext() {
        return sqlContext;
    }

    @Override
    public void execute(String sql) throws VerdictException {
        VerdictLogger.debug(this, "An input query:");
        VerdictLogger.debugPretty(this, sql, "  ");
        Query vq = Query.getInstance(this, sql);
        df = vq.computeDataFrame();
    }

    @Override
    public ResultSet getResultSet() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DataFrame getDataFrame() {
        return df;
    }
}
