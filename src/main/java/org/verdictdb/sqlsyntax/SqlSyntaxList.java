package org.verdictdb.sqlsyntax;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class SqlSyntaxList {
  
  // case-insensitive key: https://stackoverflow.com/questions/8236945/case-insensitive-string-as-hashmap-key
  static Map<String, SqlSyntax> nameToSyntax = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
  
  static {
    nameToSyntax.put("MySQL", new MysqlSyntax());
    nameToSyntax.put("PostgreSQL", new PostgresqlSyntax());
    nameToSyntax.put("Hive", new HiveSyntax());
    nameToSyntax.put("Spark", new SparkSyntax());
    nameToSyntax.put("H2", new H2Syntax());
    nameToSyntax.put("Impala", new ImpalaSyntax());
    nameToSyntax.put("SQLite", new SqliteSyntax());
  }
  
  public static SqlSyntax getSyntaxFor(String dbName) {
    return nameToSyntax.get(dbName);
  }

}
