package org.verdictdb.core.connection;

import org.apache.spark.sql.types.*;

import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.*;


public class SparkDataTypeConverter {

  public static int typeInt(String type) {
    if (type.equals("BinaryType")) {
      return BIT;
    } else if (type.equals("BooleanType")) {
      return BOOLEAN;
    } else if (type.equals("DateType")) {
      return DATE;
    } else if (type.equals("StringType")) {
      return VARCHAR;
    } else if (type.equals("TimestampType")) {
      return TIMESTAMP;
    } else if (type.equals("DecimalType")) {
      return DECIMAL;
    } else if (type.equals("DoubleType")) {
      return DOUBLE;
    } else if (type.equals("FloatType")) {
      return FLOAT;
    } else if (type.equals("ByteType")) {
      return SMALLINT;
    } else if (type.equals("IntegerType")) {
      return INTEGER;
    } else if (type.equals("LongType")) {
      return INTEGER;
    } else if (type.equals("ShortType")) {
      return SMALLINT;
    } else if (type.equals("ArrayType")) {
      return ARRAY;
    } else return OTHER;
  }
}
