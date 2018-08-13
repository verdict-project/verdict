/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.commons;

import com.rits.cloning.Cloner;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Created by Dong Young Yoon on 8/9/18. */
public class VerdictOption {

  private static final String VERDICT_TEMP_TABLE_PREFIX = "verdictdbtemptable";
  private static final String DEFAULT_META_SCHEMA_NAME = "verdictdbmeta";
  private static final String DEFAULT_TEMP_SCHEMA_NAME = "verdictdbtemp";

  private static final String DEFAULT_CONSOLE_LOG_LEVEL = "info";
  private static final String DEFAULT_FILE_LOG_LEVEL = "debug";

  private String verdictMetaSchemaName = DEFAULT_META_SCHEMA_NAME;
  private String verdictTempSchemaName = DEFAULT_TEMP_SCHEMA_NAME;
  private String verdictConsoleLogLevel = DEFAULT_CONSOLE_LOG_LEVEL;
  private String verdictFileLogLevel = DEFAULT_FILE_LOG_LEVEL;

  public VerdictOption() {}

  /**
   * Performs a deepcopy of current object
   *
   * @return a deepcopy of the current object
   */
  public VerdictOption copy() {
    return new Cloner().deepClone(this);
  }

  public String getVerdictMetaSchemaName() {
    return verdictMetaSchemaName;
  }

  public void setVerdictMetaSchemaName(String verdictMetaSchemaName) {
    this.verdictMetaSchemaName = verdictMetaSchemaName;
  }

  public String getVerdictTempSchemaName() {
    return verdictTempSchemaName;
  }

  public String getVerdictConsoleLogLevel() {
    return verdictConsoleLogLevel;
  }

  public void setVerdictConsoleLogLevel(String level) {
    this.verdictConsoleLogLevel = level;
    VerdictDBLogger.setConsoleLogLevel(level);
  }

  public String getVerdictFileLogLevel() {
    return verdictFileLogLevel;
  }

  public void setVerdictFileLogLevel(String level) {
    this.verdictFileLogLevel = level;
    VerdictDBLogger.setFileLogLevel(level);
  }

  public void setVerdictTempSchemaName(String verdictTempSchemaName) {
    this.verdictTempSchemaName = verdictTempSchemaName;
  }

  public static String getVerdictTempTablePrefix() {
    return VERDICT_TEMP_TABLE_PREFIX;
  }

  public static String getDefaultMetaSchemaName() {
    return DEFAULT_META_SCHEMA_NAME;
  }

  public static String getDefaultTempSchemaName() {
    return DEFAULT_TEMP_SCHEMA_NAME;
  }

  public static String getDefaultConsoleLogLevel() {
    return DEFAULT_CONSOLE_LOG_LEVEL;
  }

  public static String getDefaultFileLogLevel() {
    return DEFAULT_FILE_LOG_LEVEL;
  }

  public void parseConnectionString(String str) {
    String[] tokens = str.split("[&;?]");
    String pattern = "\\w+=\\w+";
    Pattern p = Pattern.compile(pattern);
    for (String token : tokens) {
      Matcher m = p.matcher(token);
      if (m.matches()) {
        String[] option = token.split("=");
        switch (option[0].toLowerCase()) {
          case "verdictdbmetaschema":
            this.setVerdictMetaSchemaName(option[1]);
            break;
          case "verdictdbtempschema":
            this.setVerdictTempSchemaName(option[1]);
            break;
          case "loglevel":
            this.setVerdictConsoleLogLevel(option[1]);
            break;
          case "file_loglevel":
            this.setVerdictFileLogLevel(option[1]);
            break;
          default:
            break;
        }
      }
    }
  }

  public void parseProperties(Properties prop) {
    // Get properties here
    String newVerdictMetaSchemaName = prop.getProperty("verdictdbmetaschema");
    String newVerdictTempSchemaName = prop.getProperty("verdictdbtempschema");

    // Set them if properties exist
    if (newVerdictMetaSchemaName != null) verdictMetaSchemaName = newVerdictMetaSchemaName;
    if (newVerdictTempSchemaName != null) verdictTempSchemaName = newVerdictTempSchemaName;
  }
}
