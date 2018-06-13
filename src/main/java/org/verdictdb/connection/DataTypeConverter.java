package org.verdictdb.connection;

import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CHAR;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATALINK;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DISTINCT;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGNVARCHAR;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NCHAR;
import static java.sql.Types.NCLOB;
import static java.sql.Types.NULL;
import static java.sql.Types.NUMERIC;
import static java.sql.Types.NVARCHAR;
import static java.sql.Types.REAL;
import static java.sql.Types.REF;
import static java.sql.Types.ROWID;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.SQLXML;
import static java.sql.Types.STRUCT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;

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
    intToStringMap.put(LONGVARCHAR, "longvarJdchar");
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
