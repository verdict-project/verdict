package org.verdictdb.connection;

import static java.sql.Types.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class DataTypeConverter {
  
  static final Map<Integer, String> intToStringMap;
  
  static final Map<String, Integer> stringToIntMap;
  
  static {
    intToStringMap = new HashMap<>();
    intToStringMap.put(ARRAY, "array");
    intToStringMap.put(BIGINT, "bigint");
    intToStringMap.put(BINARY, "binary");
    intToStringMap.put(BIT, "bit");
    intToStringMap.put(BLOB, "blob");
    intToStringMap.put(BOOLEAN, "boolean");
    intToStringMap.put(CHAR, "char");
    intToStringMap.put(CLOB, "clob");
    intToStringMap.put(DATALINK, "datalink");
    intToStringMap.put(DATE, "date");
    intToStringMap.put(DECIMAL, "decimal");
    intToStringMap.put(DISTINCT, "distinct");
    intToStringMap.put(FLOAT, "float");
    intToStringMap.put(INTEGER, "integer");
    intToStringMap.put(LONGNVARCHAR, "longnvarchar");
    intToStringMap.put(LONGVARBINARY, "longvarbinary");
    intToStringMap.put(LONGVARCHAR, "longvarchar");
    intToStringMap.put(NCHAR, "nchar");
    intToStringMap.put(NCLOB, "nclob");
    intToStringMap.put(NULL, "null");
    intToStringMap.put(NUMERIC, "numeric");
    intToStringMap.put(NVARCHAR, "nvarchar");
    intToStringMap.put(REAL, "real");
    intToStringMap.put(REF, "ref");
    intToStringMap.put(ROWID, "rowid");
    intToStringMap.put(SMALLINT, "smallint");
    intToStringMap.put(SQLXML, "xml");
    intToStringMap.put(STRUCT, "struct");
    intToStringMap.put(TIME, "time");
    intToStringMap.put(TIMESTAMP, "timestamp");
    intToStringMap.put(TINYINT, "tinyint");
    intToStringMap.put(VARBINARY, "varbinary");
    intToStringMap.put(VARCHAR, "varchar");
    
    stringToIntMap = new HashMap<>();
    for (Entry<Integer, String> pair : intToStringMap.entrySet()) {
      stringToIntMap.put(pair.getValue(), pair.getKey());
    }
  }

  public static String typeName(int inttype) {
    return intToStringMap.get(inttype);
  }
  
  public static int typeInt(String typename) {
    return stringToIntMap.get(typename.toLowerCase());
  }
}
