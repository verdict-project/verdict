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


import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.util.VerdictLogger;

public class VerdictConf {

    private Map<String, String> configs = new TreeMap<String, String>();

    private final Map<String, String> configKeySynonyms =
            new ImmutableMap.Builder<String, String>()
            .put("bypass", "verdict.bypass")
            .put("loglevel", "verdict.loglevel")
            .put("user", "verdict.jdbc.user")
            .put("password", "verdict.jdbc.password")
            .put("principal", "verdict.jdbc.kerberos_principal")
            .build();

    private final String DEFAULT_CONFIG_FILE = "verdict_default.properties";

    private final String USER_CONFIG_FILE = "verdict.properties";

    public VerdictConf() {
        this(true);
    }

    public VerdictConf(boolean resetProperties) {
        if (resetProperties) { 
            setDefaults();
            setUserConfig();
        }
        //        VerdictLogger.info("Verdict's log level set to: " + get("loglevel"));
    }

    public VerdictConf(String propertyFileName) {
        this(true);
        updateFromPropertyFile(propertyFileName);
    }

    public VerdictConf(Properties properties) {
        this(true);
        setProperties(properties);
    }

    private void setDefaults() {
        updateFromPropertyFile(DEFAULT_CONFIG_FILE);
    }

    private void setUserConfig() {
        updateFromPropertyFile(USER_CONFIG_FILE);
    }

    public void setProperties(Properties properties) {
        for (String prop : properties.stringPropertyNames()) {
            String value = properties.getProperty(prop);
            set(prop, value);
            //            if (value.length() > 0) {
            //                set(prop, value);
            //            }
        }
    }

    private void updateFromPropertyFile(String propertyFileName) {
        try {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream(propertyFileName);
            if (is == null) {
                return;
            }
            Properties p = new Properties();
            p.load(is);
            is.close();
            setProperties(p);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<String, String> getConfigs() {
        return configs;
    }

    public int getInt(String key) {
        return Integer.parseInt(get(key));
    }

    public boolean getBoolean(String key) {
        String val = get(key);
        if (val == null)
            return false;
        val = val.toLowerCase();
        return val.equals("on") || val.equals("yes") || val.equals("true") || val.equals("1");
    }

    public double getDouble(String key) {
        return Double.parseDouble(get(key));
    }

    public double getPercent(String key) {
        String val = get(key);
        if (val.endsWith("%"))
            return Double.parseDouble(val.substring(0, val.length() - 1)) / 100;
        return Double.parseDouble(val);
    }

    public String get(String key) {
        if (configKeySynonyms.containsKey(key)) {
            return get(configKeySynonyms.get(key));
        }
        return configs.get(key);
    }

    public String getOr(String key, Object defaultValue) {
        if (configs.containsKey(key)) {
            return configs.get(key);
        } else {
            return defaultValue.toString();
        }
    }

    public VerdictConf set(String keyVal) {
        int equalIndex = keyVal.indexOf('=');
        if (equalIndex == -1)
            return this;
        String key = keyVal.substring(0, equalIndex).trim();
        String val = keyVal.substring(equalIndex + 1).trim();
        return set(key, val);
    }

    public VerdictConf set(String key, String value) {
        if (value.startsWith("\"") && value.endsWith("\""))
            value = value.substring(1, value.length() - 1);

        if (configKeySynonyms.containsKey(key)) {
            return set(configKeySynonyms.get(key), value);
        }

        if (key.equals("verdict.loglevel")) {
            VerdictLogger.setLogLevel(value);
        }

        configs.put(key, value);
        return this;
    }

    public Properties toProperties() {
        Properties p = new Properties();
        for (String key : configs.keySet()) {
            p.setProperty(key, configs.get(key));
        }
        return p;
    }

    public boolean doesContain(String key) {
        return configs.containsKey(key);
    }

    /*
     * Helpers
     */

    // data DBMS
    public void setDbmsSchema(String schema) {
        configs.put("verdict.jdbc.schema", schema);
    }

    public String getDbmsSchema() {
        return get("verdict.jdbc.schema");
    }

    public void setDbms(String name) {
        set("verdict.jdbc.dbname", name);
    }

    public String getDbms() {
        return get("verdict.jdbc.dbname");
    }

    public String getDbmsClassName() {
        return get("verdict.jdbc." + getDbms() + ".class_name");
    }

    public void setHost(String host) {
        set("verdict.jdbc.host", host);
    }

    public String getHost() {
        return get("verdict.jdbc.host");
    }

    public void setUser(String user) {
        set("verdict.jdbc.user", user);
    }

    public String getUser() {
        return get("verdict.jdbc.user");
    }

    public void setPassword(String password) {
        set("verdict.jdbc.password", password);
    }

    public String getPassword() {
        return get("verdict.jdbc.password");
    }

    public boolean ignoreUserCredentials() {
        return getBoolean("verdict.jdbc.ignore_user_credentials");
    }

    public void setPort(String port) {
        set("verdict.jdbc.port", port);
    }

    public String getPort() {
        return get("verdict.jdbc.port");
    }

    public String getDefaultPort() {
        return get("verdict.jdbc." + getDbms() + ".default_port");
    }

    public double errorBoundConfidenceInPercentage() {
        return getPercent("verdict.error_bound.confidence_internal_probability");
    }

    public double getRelativeTargetCost() {
        return getPercent("verdict.relative_target_cost");
    }

    public boolean cacheSparkSamples() {
        return getBoolean("verdict.spark.cache_samples");
    }

    public String errorBoundMethod() {
        return get("verdict.error_bound.method");
    }

    public int subsamplingPartitionCount() {
        return getInt("verdict.error_bound.subsampling.partition_count");
    }

    public String subsamplingPartitionColumn() {
        return get("verdict.error_bound.subsampling.partition_column");
    }

    public String subsamplingProbabilityColumn() {
        return get("verdict.error_bound.subsampling.probability_column");
    }

    public String metaNameTableName() {
        return get("verdict.meta_data.meta_name_table");
    }

    public String metaSizeTableName() {
        return get("verdict.meta_data.meta_size_table");
    }

    public String metaRefreshPolicy() {
        return get("verdict.meta_data.refresh_policy");
    }

    public String metaDatabaseSuffix() {
        return get("verdict.meta_data.meta_database_suffix");
    }

    public String bootstrappingRandomValueColumn() {
        return get("verdict.error_bound.bootstrapping.random_value_column_name");
    }

    public String bootstrappingMultiplicityColumn() {
        return get("verdict.error_bound.bootstrapping.bootstrap_multiplicity_colname");
    }

    public boolean bypass() {
        return getBoolean("verdict.bypass");
    }

    public boolean isJdbcKerberosSet() {
        return (getJdbcKerberos().equals("n/a"))? false : true;
    }

    public String getJdbcKerberos() {
        return get("verdict.jdbc.kerberos_principal");
    }

    public boolean areSamplesStoredAsParquet() {
        if (getDbms().equals("redshift")) {
            return false;
        } else {
            return (getParquetSamples().equals("true"))? true : false;
        }
    }

    public String getParquetSamples() {
        return get("verdict.parquet_sample");
    }
    
    /**
     * These are the probabilities for ensuring at least 10 tuples.
     */
    protected static List<Pair<Integer, Double>> minSamplingProbForStratifiedSamplesMin10
                     = new ImmutableList.Builder<Pair<Integer, Double>>()
                           .add(Pair.of(100, 0.203759))
                           .add(Pair.of(50, 0.376508))
                           .add(Pair.of(40, 0.452739))
                           .add(Pair.of(30, 0.566406))
                           .add(Pair.of(20, 0.749565))
                           .add(Pair.of(15, 0.881575))
                           .add(Pair.of(14, 0.910660))
                           .add(Pair.of(13, 0.939528))
                           .add(Pair.of(12, 0.966718))
                           .add(Pair.of(11, 0.989236))
                           .build();
    
    /**
     * These are the probabilities for ensuring at least 100 tuples.
     */
    protected static List<Pair<Integer, Double>> minSamplingProbForStratifiedSamplesMin100
                     = new ImmutableList.Builder<Pair<Integer, Double>>()
                           .add(Pair.of(900, 0.140994))
                           .add(Pair.of(800, 0.158239))
                           .add(Pair.of(700, 0.180286))
                           .add(Pair.of(600, 0.209461))
                           .add(Pair.of(500, 0.249876))
                           .add(Pair.of(400, 0.309545))
                           .add(Pair.of(300, 0.406381))
                           .add(Pair.of(200, 0.589601))
                           .add(Pair.of(150, 0.756890))
                           .add(Pair.of(140, 0.801178))
                           .add(Pair.of(130, 0.849921))
                           .add(Pair.of(120, 0.902947))
                           .add(Pair.of(110, 0.958229))
                           .build();
    
    public List<Pair<Integer, Double>> samplingProbabilitiesForStratifiedSamples() {
        return minSamplingProbForStratifiedSamplesMin10;
    }
}
