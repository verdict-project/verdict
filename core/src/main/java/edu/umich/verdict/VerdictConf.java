package edu.umich.verdict;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

import org.json.JSONObject;
import org.json.JSONTokener;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.util.VerdictLogger;

public class VerdictConf {

	private Map<String, String> configs = new TreeMap<String, String>();

	private final Map<String, String> configKeySynonyms =
			new ImmutableMap.Builder<String, String>()
			.put("byapss", "verdict.bypass")
			.put("loglevel", "verdict.loglevel")
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

	public VerdictConf(File file) throws FileNotFoundException {
		this();
		updateFromStream(new FileInputStream(file));
	}

	private VerdictConf setDefaults() {
		try {
			ClassLoader cl = this.getClass().getClassLoader();
			updateFromStream(cl.getResourceAsStream("default.conf"));

			updateFromJson(cl.getResourceAsStream(DEFAULT_CONFIG_FILE));            
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
		}

		return this;
	}

	private VerdictConf updateFromStream(InputStream stream) throws FileNotFoundException {
		Scanner scanner = new Scanner(stream);
		while (scanner.hasNext()) {
			String line = scanner.nextLine();
			if (!line.isEmpty() && !line.startsWith("#"))
				set(line);
		}
		scanner.close();
		return this;
	}

	private HashMap<String, String> updateFromJsonHelper(JSONObject jsonConfig, String prefix) {
		Set<String> keys = jsonConfig.keySet();
		HashMap<String, String> temp = new HashMap<String, String>();

		if (prefix != null && !prefix.equals("")) {
			prefix += ".";
		}

		for(String key: keys) {
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
				temp.put(prefix+key, jsonConfig.getString(key));
			}
		}
		return temp;
	}

	private void updateFromJson(InputStream stream) throws FileNotFoundException {
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

	private VerdictConf set(String keyVal) {
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
		configs.put("schema", schema);
	}

	public String getDbmsSchema() {
		return get("schema");
	}

	public void setDbms(String name) {
		set("dbms", name);
	}

	public String getDbms() {
		return get("dbms");
	}

	public void setHost(String host) {
		set("host", host);
	}

	public String getHost() {
		return get("host");
	}

	public void setUser(String user) {
		set("user", user);
	}

	public String getUser() {
		return get("user");
	}

	public void setPassword(String password) {
		set("password", password);
	}

	public String getPassword() {
		return get("password");
	}

	public void setPort(String port) {
		set("port", port);
	}

	public String getPort() {
		return get("port");
	}

	public double errorBoundConfidenceInPercentage() {
		String p = get("verdict.confidence_internal_probability").replace("%", "");
		return Double.valueOf(p);
	}

	public int subsamplingPartitionCount() {
		return getInt("verdict.subsampling_partition_count");
	}

	public double getRelativeTargetCost() {
		return getDouble("verdict.relative_target_cost");
	}

	public boolean cacheSparkSamples() {
		return getBoolean("verdict.cache_spark_samples");
	}

	public String partitionColumnName() {
		return get("verdict.subsampling_partition_column_name");
	}

	public int partitionCount() {
		return getInt("verdict.subsampling_partition_count");
	}

	public String samplingProbabilityColumnName() {
		return get("verdict.sampling_probability_column");
	}
}
