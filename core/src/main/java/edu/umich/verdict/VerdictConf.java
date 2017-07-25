package edu.umich.verdict;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.json.JSONObject;
import edu.umich.verdict.json.JSONTokener;
import edu.umich.verdict.util.VerdictLogger;

public class VerdictConf {

	private Map<String, String> configs = new TreeMap<String, String>();

	private final Map<String, String> configKeySynonyms =
			new ImmutableMap.Builder<String, String>()
			.put("byapss", "verdict.bypass")
			.put("loglevel", "verdict.loglevel")
			.put("user", "verdict.dbms.user")
			.put("password", "verdict.dbms.password")
			.build();

	private final String DEFAULT_CONFIG_FILE = "default_verdict_conf.json";

	public VerdictConf() {
		setDefaults();
	}

	public VerdictConf(String jsonFilename) {
		this();
		try {
			ClassLoader cl = this.getClass().getClassLoader();
			updateFromJson(cl.getResourceAsStream(jsonFilename));
		} catch (FileNotFoundException e) {
			VerdictLogger.error(e.getMessage());
		}
	}

	public VerdictConf(Properties properties) {
		this();
		setProperties(properties);
	}

	public Map<String, String> getConfigs() {
		return configs;
	}

	public void setProperties(Properties properties) {
		for (String prop : properties.stringPropertyNames()) {
			this.set(prop, properties.getProperty(prop));
		}
	}

	public VerdictConf(File jsonFilename) throws FileNotFoundException {
		this();
		updateFromJson(new FileInputStream(jsonFilename));
	}

	private VerdictConf setDefaults() {
		try {
			ClassLoader cl = this.getClass().getClassLoader();
//			updateFromStream(cl.getResourceAsStream("default.conf"));

			updateFromJson(cl.getResourceAsStream(DEFAULT_CONFIG_FILE));            
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
		}

		return this;
	}

//	public VerdictConf updateFromStream(InputStream stream) throws FileNotFoundException {
//		Scanner scanner = new Scanner(stream);
//		while (scanner.hasNext()) {
//			String line = scanner.nextLine();
//			if (!line.isEmpty() && !line.startsWith("#"))
//				set(line);
//		}
//		scanner.close();
//		return this;
//	}

	private HashMap<String, String> updateFromJsonHelper(JSONObject jsonConfig, String prefix) {
		Set<String> keys = jsonConfig.keySet();
		HashMap<String, String> temp = new HashMap<String, String>();

		if (prefix != null && !prefix.equals("")) {
			prefix += ".";
		}

		for (String key : keys) {
			Object subObj = jsonConfig.get(key);

			if (subObj instanceof JSONObject) {
				JSONObject sub = (JSONObject) subObj;
				HashMap<String, String> sublevel = updateFromJsonHelper(sub, key);
				Set<String> subKeys = sublevel.keySet();
				for(String subKey: subKeys) {
					temp.put(prefix+subKey, sublevel.get(subKey));
				}
			}
			else {
				temp.put(prefix+key, jsonConfig.get(key).toString());
			}
		}
		
		return temp;
	}

	public void updateFromJson(InputStream stream) throws FileNotFoundException {
		JSONTokener jsonTokener = new JSONTokener(stream);
		JSONObject jsonConfig = new JSONObject(jsonTokener);
		HashMap<String, String> newConfigs = updateFromJsonHelper(jsonConfig, "");

		for (Map.Entry<String, String> e : newConfigs.entrySet()) {
			String key = e.getKey();
			String value = e.getValue();
			set(key, value);
		}
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
		return configs.get(key.toLowerCase());
	}

	public String getOr(String key, Object defaultValue) {
		if (configs.containsKey(key.toLowerCase())) {
			return configs.get(key.toLowerCase());
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
		if (val.startsWith("\"") && val.endsWith("\""))
			val = val.substring(1, val.length() - 1);
		return set(key, val);
	}

	public VerdictConf set(String key, String value) {
		key = key.toLowerCase();

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
		return configs.containsKey(key.toLowerCase());
	}

	/*
	 * Helpers
	 */

	// data DBMS
	public void setDbmsSchema(String schema) {
		configs.put("verdict.dbms.schema", schema);
	}

	public String getDbmsSchema() {
		return get("verdict.dbms.schema");
	}

	public void setDbms(String name) {
		set("verdict.dbms.dbname", name);
	}

	public String getDbms() {
		return get("verdict.dbms.dbname");
	}
	
	public String getDbmsClassName() {
		return get("verdict.dbms." + getDbms() + ".jdbc_class_name");
	}

	public void setHost(String host) {
		set("verdict.dbms.host", host);
	}

	public String getHost() {
		return get("verdict.dbms.host");
	}

	public void setUser(String user) {
		set("verdict.dbms.user", user);
	}

	public String getUser() {
		return get("verdict.dbms.user");
	}

	public void setPassword(String password) {
		set("verdict.dbms.password", password);
	}

	public String getPassword() {
		return get("verdict.dbms.password");
	}
	
	public boolean ignoreUserCredentials() {
		return getBoolean("verdict.ignore_user_credentials");
	}

	public void setPort(String port) {
		set("verdict.dbms.port", port);
	}

	public String getPort() {
		return get("verdict.dbms.port");
	}
	
	public String getDefaultPort(String dbms) {
		return get("verdict.dbms." + dbms + ".port");
	}

	public double errorBoundConfidenceInPercentage() {
		String p = get("verdict.confidence_internal_probability").replace("%", "");
		return Double.valueOf(p);
	}

	public double getRelativeTargetCost() {
		return getDouble("verdict.relative_target_cost");
	}

	public boolean cacheSparkSamples() {
		return getBoolean("verdict.spark.cache_samples");
	}

	public int subsamplingPartitionCount() {
		return getInt("verdict.subsampling.partition_count");
	}

	public String subsamplingPartitionColumn() {
		return get("verdict.subsampling.partition_column");
	}

	public String subsamplingProbabilityColumn() {
		return get("verdict.subsampling.probability_column");
	}
	
//	public int partitionCount() {
//		return getInt("verdict.subsampling.partition_count");
//	}

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
		return get("verdict.bootstrapping.random_value_column_name");
	}
	
	public String bootstrappingMultiplicityColumn() {
		return get("verdict.bootstrapping.bootstrap_multiplicity_colname");
	}
	
	public boolean bypass() {
		return getBoolean("verdict.bypass");
	}
}
